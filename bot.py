import asyncio
import logging
from datetime import datetime
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from telegram import Bot
import json

CONFIG_FILE = "config.json"

def load_config():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HeikenAshiBot:
    def __init__(self, config):
        self.config = config
        self.exchange = getattr(ccxt, config["exchange"])({'enableRateLimit': True})
        self.last_signal = {}
        self.all_symbols = []

    async def load_all_symbols(self):
        """Загружает только фьючерсные USDT пары"""
        try:
            markets = await self.exchange.load_markets()
            self.all_symbols = []
            for symbol, market in markets.items():
                # Оставляем только swap (бессрочные фьючерсы) и USDT-пары
                if market['swap'] and symbol.endswith('/USDT:USDT'):
                    self.all_symbols.append(symbol)
            logger.info(f"✅ Загружено {len(self.all_symbols)} фьючерсных монет")
            bot = Bot(token=self.config["telegram_token"])
            await bot.send_message(
                chat_id=self.config["telegram_chat_id"],
                text=f"✅ Бот запущен (только фьючерсы)\n"
                     f"Биржа: {self.config['exchange']}\n"
                     f"Таймфрейм: {self.config['timeframe']}\n"
                     f"Мониторинг {len(self.all_symbols)} монет\n\n"
                     f"_Жду сигналов по стратегии: закрытие свечи → откат → вход_",
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Ошибка загрузки списка монет: {e}")

    def calculate_heiken_ashi(self, df):
        ha_close = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        ha_open = [df['open'].iloc[0]]
        for i in range(1, len(df)):
            ha_open.append((ha_open[i-1] + ha_close.iloc[i-1]) / 2)
        ha_high = [max(df['high'].iloc[i], ha_open[i], ha_close.iloc[i]) for i in range(len(df))]
        ha_low = [min(df['low'].iloc[i], ha_open[i], ha_close.iloc[i]) for i in range(len(df))]
        ha_color = ['green' if ha_close.iloc[i] >= ha_open[i] else 'red' for i in range(len(df))]
        return {
            'ha_open': ha_open,
            'ha_close': ha_close.tolist(),
            'ha_high': ha_high,
            'ha_low': ha_low,
            'ha_color': ha_color
        }

    async def get_market_data(self, symbol, limit=30):
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, self.config["timeframe"], limit=limit)
            return pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        except Exception as e:
            logger.error(f"Ошибка данных для {symbol}: {e}")
            return None

    def check_signal(self, df, ha_data):
        if len(df) < 5:
            return None
        prev_color = ha_data['ha_color'][-3]
        signal_color = ha_data['ha_color'][-2]
        current_candle = df.iloc[-1]
        current_ha_open = (ha_data['ha_open'][-2] + ha_data['ha_close'][-2]) / 2

        # LONG: предыдущая красная, сигнальная зелёная (закрыта)
        if prev_color == 'red' and signal_color == 'green':
            if current_candle['low'] < current_ha_open:
                return {'direction': 'LONG', 'entry_price': current_candle['close'], 'timestamp': datetime.now()}
        # SHORT: предыдущая зелёная, сигнальная красная (закрыта)
        elif prev_color == 'green' and signal_color == 'red':
            if current_candle['high'] > current_ha_open:
                return {'direction': 'SHORT', 'entry_price': current_candle['close'], 'timestamp': datetime.now()}
        return None

    async def send_signal(self, signal, symbol):
        bot = Bot(token=self.config["telegram_token"])
        emoji = "🟢" if signal['direction'] == 'LONG' else "🔴"
        direction_ru = "ПОКУПКА" if signal['direction'] == 'LONG' else "ПРОДАЖА"
        message = (
            f"{emoji} **СИГНАЛ НА {direction_ru}**\n\n"
            f"Монета: {symbol}\n"
            f"Таймфрейм: {self.config['timeframe']}\n"
            f"Цена входа: `{signal['entry_price']:.5f}`\n\n"
            f"Время: {signal['timestamp'].strftime('%H:%M:%S')}"
        )
        await bot.send_message(chat_id=self.config["telegram_chat_id"], text=message, parse_mode='Markdown')
        logger.info(f"Сигнал {signal['direction']} для {symbol} по {signal['entry_price']}")

    def is_duplicate(self, symbol, signal):
        key = f"{symbol}_{signal['direction']}"
        if key not in self.last_signal:
            return False
        last_time = self.last_signal[key]['time']
        last_price = self.last_signal[key]['price']
        time_diff = (signal['timestamp'] - last_time).total_seconds()
        price_diff = abs(signal['entry_price'] - last_price) / last_price if last_price != 0 else 1
        if time_diff < 300 and price_diff < 0.002:
            return True
        return False

    async def run(self):
        await self.load_all_symbols()
        logger.info(f"🚀 Мониторинг {len(self.all_symbols)} фьючерсных монет на {self.config['timeframe']}")
        
        while True:
            for symbol in self.all_symbols:
                try:
                    df = await self.get_market_data(symbol, limit=30)
                    if df is not None and len(df) >= 5:
                        ha_data = self.calculate_heiken_ashi(df)
                        signal = self.check_signal(df, ha_data)
                        if signal and not self.is_duplicate(symbol, signal):
                            await self.send_signal(signal, symbol)
                            self.last_signal[f"{symbol}_{signal['direction']}"] = {
                                'time': signal['timestamp'],
                                'price': signal['entry_price']
                            }
                    await asyncio.sleep(0.3)
                except Exception as e:
                    logger.error(f"Ошибка {symbol}: {e}")
            await asyncio.sleep(60)

    async def close(self):
        await self.exchange.close()

async def main():
    config = load_config()
    bot = HeikenAshiBot(config)
    try:
        await bot.run()
    finally:
        await bot.close()

if __name__ == "__main__":
    asyncio.run(main())
