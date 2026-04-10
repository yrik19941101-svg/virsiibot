import asyncio
import logging
import requests
import pandas as pd
from telegram import Bot
import json
from datetime import datetime

CONFIG_FILE = "config.json"

def load_config():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HeikenAshiBot:
    def __init__(self, config):
        self.config = config
        self.state = {}

    async def load_symbols(self):
        self.all_symbols = self.config['symbols']
        bot = Bot(token=self.config["telegram_token"])
        await bot.send_message(
            chat_id=self.config["telegram_chat_id"],
            text=f"✅ Бот запущен (CoinGecko, без дублей)\n"
                 f"Таймфрейм: {self.config['timeframe']}\n"
                 f"Мониторинг: {len(self.all_symbols)} монет\n\n"
                 f"_Стратегия: смена цвета HA → откат → сигнал_",
            parse_mode='Markdown'
        )

    @staticmethod
    def calculate_heiken_ashi(df):
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

    async def get_market_data(self, coin_id, limit=30):
        try:
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc"
            params = {'vs_currency': 'usd', 'days': 'max'}
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            if not data:
                logger.error(f"Нет данных для {coin_id}")
                return None
            df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = df.tail(limit)
            return df
        except Exception as e:
            logger.error(f"Ошибка данных для {coin_id}: {e}")
            return None

    async def send_signal(self, symbol, direction, price):
        bot = Bot(token=self.config["telegram_token"])
        emoji = "🟢" if direction == 'LONG' else "🔴"
        direction_ru = "ПОКУПКА" if direction == 'LONG' else "ПРОДАЖА"
        message = (
            f"{emoji} **СИГНАЛ НА {direction_ru}**\n\n"
            f"Монета: {symbol}\n"
            f"Таймфрейм: {self.config['timeframe']}\n"
            f"Цена входа: `{price:.8f}`\n\n"
            f"Время: {datetime.now().strftime('%H:%M:%S')}"
        )
        await bot.send_message(chat_id=self.config["telegram_chat_id"], text=message, parse_mode='Markdown')
        logger.info(f"Сигнал {direction} для {symbol} по {price}")

    async def process_symbol(self, coin_id):
        df = await self.get_market_data(coin_id, limit=30)
        if df is None or len(df) < 10:
            return
        df = self.calculate_heiken_ashi(df)
        current_timestamp = df['timestamp'].iloc[-1]

        if coin_id not in self.state:
            self.state[coin_id] = {'last_timestamp': None, 'signal_sent': False}
        state = self.state[coin_id]

        if current_timestamp != state['last_timestamp']:
            state['last_timestamp'] = current_timestamp
            state['signal_sent'] = False

        if state['signal_sent']:
            return

        prev2_color = df['ha_color'].iloc[-3]
        prev1_color = df['ha_color'].iloc[-2]
        current_candle = df.iloc[-1]
        current_ha_open = df['ha_open'].iloc[-1]

        if prev2_color == 'red' and prev1_color == 'green':
            if current_candle['low'] < current_ha_open:
                await self.send_signal(coin_id, 'LONG', current_candle['close'])
                state['signal_sent'] = True
        elif prev2_color == 'green' and prev1_color == 'red':
            if current_candle['high'] > current_ha_open:
                await self.send_signal(coin_id, 'SHORT', current_candle['close'])
                state['signal_sent'] = True

    async def run(self):
        await self.load_symbols()
        logger.info(f"Мониторинг {len(self.all_symbols)} монет на {self.config['timeframe']}")

        while True:
            for coin_id in self.all_symbols:
                try:
                    await self.process_symbol(coin_id)
                except Exception as e:
                    logger.error(f"Ошибка {coin_id}: {e}")
                await asyncio.sleep(1.5)
            await asyncio.sleep(60)

async def main():
    config = load_config()
    bot = HeikenAshiBot(config)
    await bot.run()

if __name__ == "__main__":
    asyncio.run(main())
