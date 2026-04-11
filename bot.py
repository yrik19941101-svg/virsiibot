import asyncio
import logging
import ccxt.async_support as ccxt
import pandas as pd
import json
from datetime import datetime
from telegram import Bot

CONFIG_FILE = "config.json"

def load_config():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TradingBot:
    def __init__(self, config):
        self.config = config
        self.exchange = getattr(ccxt, config["exchange"])({
            'enableRateLimit': True,
            'apiKey': config['api_key'],
            'secret': config['api_secret'],
            'options': {
                'defaultType': 'swap',
                'adjustForTimeDifference': True
            }
        })
        self.next_trade_doubled = False
        self.open_positions = set()
        self.pos_data = {}
        self.all_symbols = self.config['symbols']
        self.signal_state = {}
        self.telegram_bot = Bot(token=config["telegram_token"])

    async def get_balance(self):
        try:
            balance = await self.exchange.fetch_balance()
            return balance['USDT']['free']
        except Exception as e:
            logger.error(f"Ошибка баланса: {e}")
            return 0.0

    async def send_telegram(self, message):
        try:
            await self.telegram_bot.send_message(chat_id=self.config["telegram_chat_id"], text=message, parse_mode=None)
        except Exception as e:
            logger.error(f"Ошибка Telegram: {e}")

    async def load_markets(self):
        await self.exchange.load_markets()
        logger.info(f"Загружено рынков, используем {len(self.all_symbols)} пар")
        logger.info(f"Таймфрейм: {self.config['timeframe']}")

    def get_trade_amount(self):
        base = self.config['trade_params']['fixed_trade_amount']
        return base * 2 if self.next_trade_doubled else base

    async def open_position(self, symbol, direction, price, volume):
        if len(self.open_positions) >= self.config['max_positions']:
            logger.warning(f"Лимит позиций ({self.config['max_positions']}) достигнут, пропускаем {symbol}")
            return

        trade_amount = self.get_trade_amount()
        leverage = self.config['trade_params']['default_leverage']
        side = 'buy' if direction == 'LONG' else 'sell'
        quantity = (trade_amount * leverage) / price
        quantity = round(quantity, 5)
        if quantity <= 0:
            logger.error(f"Неверное количество {symbol}: {quantity}")
            return

        logger.info(f"Расчёт: сумма {trade_amount} USDT, плечо {leverage}, цена {price} → количество {quantity}")

        try:
            sl_percent = self.config['trade_params']['sl_percent']
            tp_percent = self.config['trade_params']['tp_percent']
            if direction == 'LONG':
                stop_price = round(price * (1 - (1/leverage) * sl_percent), 5)
                take_price = round(price * (1 + (1/leverage) * tp_percent), 5)
            else:
                stop_price = round(price * (1 + (1/leverage) * sl_percent), 5)
                take_price = round(price * (1 - (1/leverage) * tp_percent), 5)

            # Повторные попытки при ошибке Oversold
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    await self.exchange.create_order(
                        symbol=symbol,
                        type='market',
                        side=side,
                        amount=quantity
                    )
                    break
                except Exception as e:
                    if '30005' in str(e) or 'Oversold' in str(e):
                        wait = 2 ** attempt
                        logger.warning(f"Oversold для {symbol}, повтор через {wait} сек (попытка {attempt+1}/{max_retries})")
                        await asyncio.sleep(wait)
                    else:
                        raise
            else:
                raise Exception(f"Не удалось открыть {symbol} после {max_retries} попыток")

            logger.info(f"🟢 ОТКРЫТА {direction} {symbol}: {quantity} по {price}, сумма {trade_amount} USDT, плечо {leverage}")
            logger.info(f"SL: {stop_price:.5f} (изм {abs(stop_price/price - 1)*100:.2f}%)")
            logger.info(f"TP: {take_price:.5f} (изм {abs(take_price/price - 1)*100:.2f}%)")

            self.open_positions.add(symbol)
            self.pos_data[symbol] = {
                'direction': direction,
                'entry_price': price,
                'quantity': quantity,
                'stop_price': stop_price,
                'take_price': take_price,
                'trade_amount': trade_amount,
                'leverage': leverage,
                'closed': False,
                'trailing_activated': False,
                'breakeven_stop': None
            }

            balance = await self.get_balance()
            emoji = "🟢" if direction == 'LONG' else "🔴"
            msg = (f"{emoji} ОТКРЫТА СДЕЛКА {direction}\n"
                   f"Монета: {symbol}\nЦена: {price:.5f}\nСумма: {trade_amount:.2f} USDT\n"
                   f"Плечо: {leverage}x\nКол-во: {quantity:.5f}\n"
                   f"SL: {stop_price:.5f} ({sl_percent*100:.0f}%)\n"
                   f"TP: {take_price:.5f} ({tp_percent*100:.0f}%)\n"
                   f"Баланс: {balance:.2f} USDT")
            await self.send_telegram(msg)

            if symbol in self.signal_state:
                self.signal_state[symbol]['waiting_for_pullback'] = False
        except Exception as e:
            logger.error(f"Ошибка открытия {symbol}: {e}")

    async def close_position(self, symbol, reason, current_price):
        pos = self.pos_data.get(symbol)
        if not pos or pos.get('closed'):
            return
        try:
            close_side = 'sell' if pos['direction'] == 'LONG' else 'buy'
            await self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=close_side,
                amount=pos['quantity']
            )
            logger.info(f"🔴 ЗАКРЫТА {symbol} по {reason}, цена {current_price}")
            self.pos_data[symbol]['closed'] = True

            balance = await self.get_balance()
            emoji = "🔴" if reason == 'stop_loss' else "🟢"
            msg = f"{emoji} СДЕЛКА ЗАКРЫТА\nМонета: {symbol}\nПричина: {reason}\nЦена: {current_price:.5f}\nБаланс: {balance:.2f} USDT"
            await self.send_telegram(msg)

            if reason == 'stop_loss':
                self.next_trade_doubled = True
                await self.send_telegram(f"⚠️ СТОП-ЛОСС\nСледующая сделка будет удвоена")
            else:
                self.next_trade_doubled = False

            self.open_positions.discard(symbol)
            asyncio.create_task(self.delayed_cleanup(symbol))
        except Exception as e:
            logger.error(f"Ошибка закрытия {symbol}: {e}")

    async def delayed_cleanup(self, symbol):
        await asyncio.sleep(10)
        if symbol in self.pos_data:
            del self.pos_data[symbol]

    async def monitor_positions(self):
        while True:
            for symbol, pos in list(self.pos_data.items()):
                if pos.get('closed'):
                    continue
                try:
                    ticker = await self.exchange.fetch_ticker(symbol)
                    cur_price = ticker['last']
                    should_close = False
                    reason = None

                    tp_percent = self.config['trade_params']['tp_percent']
                    activation = self.config['trade_params'].get('trailing_stop_activation', 0.5)
                    if not pos.get('trailing_activated'):
                        profit_percent = (cur_price - pos['entry_price']) / pos['entry_price'] if pos['direction'] == 'LONG' else (pos['entry_price'] - cur_price) / pos['entry_price']
                        if profit_percent >= tp_percent * activation:
                            pos['trailing_activated'] = True
                            pos['breakeven_stop'] = pos['entry_price']
                            logger.info(f"{symbol}: трейлинг-стоп активирован на {pos['entry_price']}")
                            await self.send_telegram(f"🔒 {symbol}: трейлинг-стоп, стоп на {pos['entry_price']:.5f}")

                    if pos.get('trailing_activated') and pos['breakeven_stop']:
                        if pos['direction'] == 'LONG' and cur_price <= pos['breakeven_stop']:
                            should_close = True
                            reason = 'trailing_stop'
                        elif pos['direction'] == 'SHORT' and cur_price >= pos['breakeven_stop']:
                            should_close = True
                            reason = 'trailing_stop'

                    if not should_close:
                        if pos['direction'] == 'LONG':
                            if cur_price <= pos['stop_price']:
                                should_close = True
                                reason = 'stop_loss'
                            elif cur_price >= pos['take_price']:
                                should_close = True
                                reason = 'take_profit'
                        else:
                            if cur_price >= pos['stop_price']:
                                should_close = True
                                reason = 'stop_loss'
                            elif cur_price <= pos['take_price']:
                                should_close = True
                                reason = 'take_profit'

                    if should_close:
                        await self.close_position(symbol, reason, cur_price)
                except Exception as e:
                    logger.error(f"Ошибка мониторинга {symbol}: {e}")
            await asyncio.sleep(2)

    def calculate_heiken_ashi(self, df):
        df = df.copy()
        df['ha_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        ha_open = [df['open'].iloc[0]]
        for i in range(1, len(df)):
            ha_open.append((ha_open[i-1] + df['ha_close'].iloc[i-1]) / 2)
        df['ha_open'] = ha_open
        df['ha_high'] = df[['high', 'ha_open', 'ha_close']].max(axis=1)
        df['ha_low'] = df[['low', 'ha_open', 'ha_close']].min(axis=1)
        df['ha_color'] = df.apply(lambda row: 'green' if row['ha_close'] >= row['ha_open'] else 'red', axis=1)
        return df

    async def get_market_data(self, symbol, limit=50):
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, self.config["timeframe"], limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            logger.error(f"Ошибка данных {symbol}: {e}")
            return None

    async def process_symbol(self, symbol):
        if symbol in self.open_positions:
            return
        logger.info(f"🔍 Проверяю монету: {symbol}")
        df = await self.get_market_data(symbol, limit=50)
        if df is None or len(df) < 20:
            return
        df = self.calculate_heiken_ashi(df)
        current_ts = df['timestamp'].iloc[-1]

        if symbol not in self.signal_state:
            self.signal_state[symbol] = {
                'last_candle_ts': None,
                'waiting_for_pullback': False,
                'signal_candle_close': None,
                'signal_direction': None,
                'signal_volume': 0
            }
        state = self.signal_state[symbol]

        if current_ts != state['last_candle_ts']:
            state['last_candle_ts'] = current_ts
            prev2 = df['ha_color'].iloc[-3]
            prev1 = df['ha_color'].iloc[-2]
            sig_candle = df.iloc[-2]
            if prev2 == 'red' and prev1 == 'green':
                state['waiting_for_pullback'] = True
                state['signal_direction'] = 'LONG'
                state['signal_candle_close'] = sig_candle['close']
                logger.info(f"{symbol}: сигнал LONG, ждём отката вниз")
            elif prev2 == 'green' and prev1 == 'red':
                state['waiting_for_pullback'] = True
                state['signal_direction'] = 'SHORT'
                state['signal_candle_close'] = sig_candle['close']
                logger.info(f"{symbol}: сигнал SHORT, ждём отката вверх")
            else:
                state['waiting_for_pullback'] = False
                state['signal_direction'] = None

        if state['waiting_for_pullback']:
            curr_candle = df.iloc[-1]
            curr_ha_open = df['ha_open'].iloc[-1]
            min_pullback = self.config['trade_params']['min_pullback_percent'] / 100.0
            if state['signal_direction'] == 'LONG':
                target_low = min(curr_ha_open, state['signal_candle_close']) * (1 - min_pullback)
                if curr_candle['low'] <= target_low:
                    await self.open_position(symbol, 'LONG', curr_candle['close'], curr_candle['volume'])
                    state['waiting_for_pullback'] = False
            elif state['signal_direction'] == 'SHORT':
                target_high = max(curr_ha_open, state['signal_candle_close']) * (1 + min_pullback)
                if curr_candle['high'] >= target_high:
                    await self.open_position(symbol, 'SHORT', curr_candle['close'], curr_candle['volume'])
                    state['waiting_for_pullback'] = False

    async def run(self):
        await self.load_markets()
        asyncio.create_task(self.monitor_positions())
        balance = await self.get_balance()
        await self.send_telegram(f"🚀 Бот запущен (MEXC, увеличены задержки)\nТаймфрейм: 30m\nСумма сделки: 2 USDT\nSL/TP: 50%\nТрейлинг-стоп: при 50% TP\nПлечо: 50x (настроено вручную)\nБаланс: {balance:.2f} USDT")
        while True:
            for symbol in self.all_symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.error(f"Ошибка {symbol}: {e}")
                await asyncio.sleep(1.5)   # увеличенная задержка между монетами
            await asyncio.sleep(120)       # увеличенная задержка между циклами

    async def close(self):
        await self.exchange.close()

async def main():
    config = load_config()
    bot = TradingBot(config)
    try:
        await bot.run()
    finally:
        await bot.close()

if __name__ == "__main__":
    asyncio.run(main())
