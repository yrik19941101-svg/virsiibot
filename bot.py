import asyncio
import logging
import ccxt.async_support as ccxt
import pandas as pd
import json
from datetime import datetime, timedelta
from telegram import Bot
from typing import Optional, Dict, Any

CONFIG_FILE = "config_signals_precise.json"

def load_config():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SignalBot:
    def __init__(self, config):
        self.config = config

        self.exchange = getattr(ccxt, config["exchange"])({
            'enableRateLimit': True,
            'apiKey': config.get('api_key', ''),
            'secret': config.get('api_secret', ''),
            'options': {
                'defaultType': 'swap',
                'adjustForTimeDifference': True
            }
        })

        self.telegram_bot = Bot(token=config["telegram_token"])

        self.all_symbols = []
        self.blacklist = set()
        self.sent_signals: Dict[str, Dict[str, Any]] = {}

        self.telegram_retry_count = config.get("telegram_retry_count", 3)
        self.telegram_retry_delay = config.get("telegram_retry_delay", 5)

        blacklist_from_config = self.config.get('blacklist_symbols', [])
        for sym in blacklist_from_config:
            self.blacklist.add(sym)
        logger.info(f"Загружено {len(blacklist_from_config)} символов в чёрный список")

    async def send_telegram_signal(self, symbol, signal_type, timeframe, price, reason=""):
        msg = (f"🎯 ТОЧНЫЙ СИГНАЛ {signal_type} ({timeframe})\n"
               f"Монета: {symbol}\n"
               f"Цена входа: {price:.5f}\n"
               f"Причина: {reason}\n"
               f"Время: {datetime.now().strftime('%H:%M:%S')}")

        for i in range(self.telegram_retry_count):
            try:
                await self.telegram_bot.send_message(
                    chat_id=self.config["telegram_chat_id"],
                    text=msg,
                    parse_mode=None
                )
                return
            except Exception as e:
                if i < self.telegram_retry_count - 1:
                    logger.warning(f"Повторная попытка Telegram {i+1}/{self.telegram_retry_count}: {e}")
                    await asyncio.sleep(self.telegram_retry_delay)
                else:
                    logger.error(f"Ошибка Telegram после {self.telegram_retry_count} попыток: {e}")

    async def get_market_data(self, symbol, timeframe, limit=20):
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            if 'pause currently' not in str(e) and 'not found' not in str(e):
                logger.error(f"Ошибка данных {symbol} ({timeframe}): {e}")
            return None

    def calculate_heiken_ashi(self, df):
        df = df.copy()
        df['ha_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        ha_open = [df['open'].iloc[0]]
        for i in range(1, len(df)):
            ha_open.append((ha_open[i-1] + df['ha_close'].iloc[i-1]) / 2)
        df['ha_open'] = ha_open
        df['ha_high'] = df[['high', 'ha_open', 'ha_close']].max(axis=1)
        df['ha_low'] = df[['low', 'ha_open', 'ha_close']].min(axis=1)
        df['ha_color'] = df.apply(
            lambda row: 'green' if row['ha_close'] >= row['ha_open'] else 'red',
            axis=1
        )
        return df

    async def fetch_all_tickers(self):
        try:
            tickers = await self.exchange.fetch_tickers()
            return tickers
        except Exception as e:
            logger.error(f"Ошибка fetch_tickers: {e}")
            return {}

    async def is_suitable_symbol(self, symbol, tickers):
        try:
            ticker = tickers.get(symbol)
            if not ticker:
                return False
            volume_24h = ticker.get('quoteVolume', 0)
            if volume_24h < self.config.get('min_volume_24h', 0):
                return False
            high = ticker.get('high', 0)
            low = ticker.get('low', 0)
            if low > 0:
                volatility = (high - low) / low * 100
                if volatility > self.config.get('volatility_filter_percent', 100):
                    return False
            return True
        except Exception as e:
            logger.error(f"Ошибка в is_suitable_symbol для {symbol}: {e}")
            return False

    async def load_market_list(self):
        await self.exchange.load_markets()
        candidates = [
            symbol
            for symbol, market in self.exchange.markets.items()
            if market['swap']
               and market['quote'] == 'USDT'
               and symbol.count('/') == 1
               and not symbol.startswith(('NCFX', 'NCCO', 'NCSI', 'NCSK'))
        ]
        logger.info(f"Найдено {len(candidates)} кандидатов для сигнального бота")

        tickers = await self.fetch_all_tickers()
        self.all_symbols = []
        for symbol in candidates:
            if symbol in self.blacklist:
                continue
            if await self.is_suitable_symbol(symbol, tickers):
                self.all_symbols.append(symbol)
        logger.info(f"Осталось {len(self.all_symbols)} монет после фильтра")

    def period_hours(self, timeframe):
        mapping = {'1m': 1/60, '3m': 3/60, '5m': 5/60, '15m': 15/60, '1h': 1,
                   '4h': 4, '6h': 6, '12h': 12, '1d': 24}
        return mapping.get(timeframe, 6)

    def is_mid_candle(self, df, timeframe):
        if len(df) < 1:
            return False
        now = pd.Timestamp.utcnow().tz_localize(None)
        last_ts = df['timestamp'].iloc[-1]
        freq_hours = self.period_hours(timeframe)
        elapsed_seconds = (now - last_ts).total_seconds()
        elapsed_hours = elapsed_seconds / 3600
        remaining_hours = freq_hours - elapsed_hours
        half_period = freq_hours / 2
        return remaining_hours > half_period

    def min_pullback_percent(self):
        return self.config['signal_params'].get('min_pullback_percent', 0.2)

    async def generate_signal(self, symbol, timeframe, limit=20):
        df = await self.get_market_data(symbol, timeframe, limit=limit)
        if df is None or len(df) < 3:
            return None

        # Проверка времени (актуально для свечи, на которой будем входить – откатной)
        # Здесь проверяем текущую свечу (последнюю)
        if not self.is_mid_candle(df, timeframe):
            return None

        ha_df = self.calculate_heiken_ashi(df)
        if len(ha_df) < 3:
            return None

        # Индексы:
        # -2: сигнальная свеча (закрыта)
        # -1: откатная свеча (текущая, незакрытая)
        sig = ha_df.iloc[-2]
        pull = ha_df.iloc[-1]

        sig_color = sig['ha_color']
        sig_low = sig['low']
        sig_high = sig['high']
        sig_ha_open = sig['ha_open']

        # Определяем направление сигнала
        signal_type = None
        reason = ""

        # LONG: сигнальная свеча зелёная (предыдущая была красной – не проверяем, но можем добавить)
        #      и откат вниз: текущая свеча имеет low ниже, чем HA_Open сигнальной?
        # По классике: откат вниз на следующей свече – проверяем pull['low'] < sig_ha_open
        if sig_color == 'green':
            pull_low = pull['low']
            if pull_low < sig_ha_open:
                pullback_percent = (sig_ha_open - pull_low) / sig_ha_open * 100
                if pullback_percent >= self.min_pullback_percent():
                    signal_type = 'LONG'
                    reason = (f"Reversal: зелёная сигнальная, откат вниз на {pullback_percent:.2f}%")
        # SHORT: сигнальная свеча красная, откат вверх
        elif sig_color == 'red':
            pull_high = pull['high']
            if pull_high > sig_ha_open:
                pullback_percent = (pull_high - sig_ha_open) / sig_ha_open * 100
                if pullback_percent >= self.min_pullback_percent():
                    signal_type = 'SHORT'
                    reason = (f"Reversal: красная сигнальная, откат вверх на {pullback_percent:.2f}%")

        if signal_type:
            price = sig['close']  # вход по цене закрытия сигнальной свечи
            return {
                'type': signal_type,
                'symbol': symbol,
                'timeframe': timeframe,
                'price': price,
                'reason': reason
            }

        return None

    async def scan_for_signals(self):
        await self.load_market_list()
        timeframes = self.config.get('timeframes', ['6h', '12h'])

        while True:
            logger.info(f"🔄 Сканирую {len(self.all_symbols)} монет по таймфреймам: {timeframes}")

            for symbol in self.all_symbols:
                if symbol in self.blacklist:
                    continue

                for tf in timeframes:
                    key = f"{symbol}_{tf}"
                    try:
                        signal = await self.generate_signal(symbol, tf)
                        if not signal:
                            continue

                        # Защита от дублей (одно направление на одной монете в течение 2 часов)
                        last_signal = self.sent_signals.get(key)
                        if last_signal and last_signal['type'] == signal['type']:
                            time_diff = (datetime.utcnow() - last_signal['ts']).total_seconds()
                            if time_diff < 7200:  # 2 часа
                                logger.debug(f"Дубль сигнала {key} ({signal['type']}) – пропущен")
                                continue

                        self.sent_signals[key] = {
                            'type': signal['type'],
                            'ts': datetime.utcnow()
                        }

                        logger.info(f"Сигнал {signal['type']} на {symbol} {tf}: {signal['price']:.5f}")
                        await self.send_telegram_signal(
                            symbol=signal['symbol'],
                            signal_type=signal['type'],
                            timeframe=signal['timeframe'],
                            price=signal['price'],
                            reason=signal['reason']
                        )
                    except Exception as e:
                        logger.error(f"Ошибка при генерации сигнала {symbol} {tf}: {e}")

            # Очистка старых записей sent_signals (старше 24 часов)
            now = datetime.utcnow()
            to_remove = [k for k, v in self.sent_signals.items() if (now - v['ts']).total_seconds() > 86400]
            for k in to_remove:
                del self.sent_signals[k]

            logger.info("Жду 30 минут до следующего цикла...")
            await asyncio.sleep(1800)

    async def run(self):
        try:
            balance = await self.exchange.fetch_balance()
            usdt_free = balance['USDT']['free']
            logger.info(f"Баланс: {usdt_free:.2f} USDT (только сигналы)")
            await self.send_telegram_signal("BOT", "СТАРТ", "LOG", 0.0,
                "Старт сигнального бота Heiken Ashi (6h/12h, откат на следующей свече)")
        except Exception as e:
            logger.error(f"Ошибка баланса: {e}")
            await self.send_telegram_signal("BOT", "СТАРТ", "LOG", 0.0,
                "Сигнальный бот запущен (без баланса)")

        await self.scan_for_signals()

    async def close(self):
        await self.exchange.close()


async def main():
    config = load_config()
    bot = SignalBot(config)
    try:
        await bot.run()
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())
