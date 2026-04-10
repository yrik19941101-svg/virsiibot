import asyncio
import logging
import time
import hashlib
import hmac
import requests
from datetime import datetime
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

# Простой клиент только для получения свечей (публичные эндпоинты, без подписи)
class BingXPublicClient:
    def __init__(self):
        self.base_url = "https://open-api.bingx.com"

    def get_klines(self, symbol, interval, limit=30):
        endpoint = "/openApi/swap/v2/market/klines"
        params = {'symbol': symbol, 'interval': interval, 'limit': limit}
        url = f"{self.base_url}{endpoint}?" + "&".join([f"{k}={v}" for k, v in params.items()])
        response = requests.get(url)
        data = response.json()
        if data.get('code') != 0:
            logger.error(f"BingX API error: {data}")
            return None
        return data.get('data')

class HeikenAshiBot:
    def __init__(self, config):
        self.config = config
        self.client = BingXPublicClient()
        self.state = {}  # для каждой монеты храним состояние сигнала

    async def load_symbols(self):
        self.all_symbols = self.config['symbols']
        bot = Bot(token=self.config["telegram_token"])
        await bot.send_message(
            chat_id=self.config["telegram_chat_id"],
            text=f"✅ Бот запущен (только сигналы, без торговли)\n"
                 f"Таймфрейм: {self.config['timeframe']}\n"
                 f"Мониторинг: {len(self.all_symbols)} монет\n\n"
                 f"_Стратегия: смена цвета HA → откат → сигнал_",
            parse_mode='Markdown'
        )

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

    async def get_market_data(self, symbol, limit=30):
        try:
            klines = self.client.get_klines(symbol, self.config['timeframe'], limit)
            if not klines:
                return None
            df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover', 'openTime', 'closeTime'])
            df = df[['open', 'high', 'low', 'close', 'timestamp']].astype(float)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            logger.error(f"Ошибка получения данных для {symbol}: {e}")
            return None

    async def send_signal(self, symbol, direction, price):
        bot = Bot(token=self.config["telegram_token"])
        emoji = "🟢" if direction == 'LONG' else "🔴"
        direction_ru = "ПОКУПКА" if direction == 'LONG' else "ПРОДАЖА"
        message = (
            f"{emoji} **СИГНАЛ НА {direction_ru}**\n\n"
            f"Монета: {symbol}\n"
            f"Таймфрейм: {self.config['timeframe']}\n"
            f"Цена входа: `{price:.5f}`\n\n"
            f"Время: {datetime.now().strftime('%H:%M:%S')}\n\n"
            f"_Стратегия: смена цвета HA → откат → вход_"
        )
        await bot.send_message(chat_id=self.config["telegram_chat_id"], text=message, parse_mode='Markdown')
        logger.info(f"СИГНАЛ {direction} для {symbol} по цене {price}")

    async def process_symbol(self, symbol):
        df = await self.get_market_data(symbol, limit=30)
        if df is None or len(df) < 10:
            return
        df = self.calculate_heiken_ashi(df)
        current_timestamp = df['timestamp'].iloc[-1]

        if symbol not in self.state:
            self.state[symbol] = {
                'last_candle_time': 0,
                'signal_sent_for_current_candle': False
            }
        state = self.state[symbol]

        # Если появилась новая свеча (закрылась предыдущая)
        if current_timestamp != state['last_candle_time']:
            state['last_candle_time'] = current_timestamp
            state['signal_sent_for_current_candle'] = False

            # Проверяем смену цвета на закрытых свечах (индексы -3 и -2)
            prev2_color = df['ha_color'].iloc[-3]
            prev1_color = df['ha_color'].iloc[-2]
            current_candle = df.iloc[-1]
            current_ha_open = df['ha_open'].iloc[-1]

            # LONG: предыдущая красная, затем закрылась зелёная
            if prev2_color == 'red' and prev1_color == 'green':
                # Ждём отката вниз на текущей свече
                if current_candle['low'] < current_ha_open:
                    await self.send_signal(symbol, 'LONG', current_candle['close'])
                    state['signal_sent_for_current_candle'] = True
                    logger.info(f"{symbol}: отправлен сигнал LONG")
            # SHORT: предыдущая зелёная, затем закрылась красная
            elif prev2_color == 'green' and prev1_color == 'red':
                if current_candle['high'] > current_ha_open:
                    await self.send_signal(symbol, 'SHORT', current_candle['close'])
                    state['signal_sent_for_current_candle'] = True
                    logger.info(f"{symbol}: отправлен сигнал SHORT")

    async def run(self):
        await self.load_symbols()
        logger.info(f"🚀 Мониторинг {len(self.all_symbols)} монет на {self.config['timeframe']}")

        while True:
            for symbol in self.all_symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.error(f"Ошибка {symbol}: {e}")
                await asyncio.sleep(0.5)  # небольшая задержка между монетами
            await asyncio.sleep(60)  # пауза между полными циклами

async def main():
    config = load_config()
    bot = HeikenAshiBot(config)
    await bot.run()

if __name__ == "__main__":
    asyncio.run(main())
