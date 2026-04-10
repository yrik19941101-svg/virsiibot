import asyncio
import logging
from datetime import datetime

import tvkit
import pandas as pd
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
        self.state = {}

    async def load_symbols(self):
        self.all_symbols = self.config['symbols']
        bot = Bot(token=self.config["telegram_token"])
        await bot.send_message(
            chat_id=self.config["telegram_chat_id"],
            text=f"✅ Бот запущен (TradingView, реальные данные)\n"
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

    async def send_signal(self, symbol, direction, price):
        bot = Bot(token=self.config["telegram_token"])
        emoji = "🟢" if direction == 'LONG' else "🔴"
        direction_ru = "ПОКУПКА" if direction == 'LONG' else "ПРОДАЖА"
        message = (
            f"{emoji} **СИГНАЛ НА {direction_ru}**\n\n"
            f"Монета: {symbol}\n"
            f"Таймфрейм: {self.config['timeframe']}\n"
            f"Цена входа: `{price:.5f}`\n\n"
            f"Время: {datetime.now().strftime('%H:%M:%S')}"
        )
        await bot.send_message(chat_id=self.config["telegram_chat_id"], text=message, parse_mode='Markdown')
        logger.info(f"Сигнал {direction} для {symbol} по {price}")

    async def monitor_symbol(self, symbol):
        try:
            async with tvkit.OHLCV() as client:
                bars = await client.get_historical_ohlcv(
                    exchange_symbol=symbol,
                    interval=self.config['timeframe'],
                    bars_count=100
                )
                if not bars:
                    logger.error(f"Нет данных для {symbol}")
                    return

                df = pd.DataFrame([{
                    'timestamp': bar.timestamp,
                    'open': bar.open,
                    'high': bar.high,
                    'low': bar.low,
                    'close': bar.close,
                } for bar in bars])
                df = self.calculate_heiken_ashi(df)
                last_timestamp = df['timestamp'].iloc[-1]
                signal_sent_for_candle = False

                async for bar in client.get_ohlcv(symbol, self.config['timeframe']):
                    current_timestamp = bar.timestamp
                    if current_timestamp != last_timestamp:
                        last_timestamp = current_timestamp
                        signal_sent_for_candle = False
                        new_row = {
                            'timestamp': bar.timestamp,
                            'open': bar.open,
                            'high': bar.high,
                            'low': bar.low,
                            'close': bar.close,
                        }
                        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                        if len(df) > 30:
                            df = df.iloc[-30:]
                        df = self.calculate_heiken_ashi(df)

                    if not signal_sent_for_candle and len(df) >= 3:
                        prev2_color = df['ha_color'].iloc[-3]
                        prev1_color = df['ha_color'].iloc[-2]
                        current_ha_open = df['ha_open'].iloc[-1]
                        current_close = df['close'].iloc[-1]

                        if prev2_color == 'red' and prev1_color == 'green':
                            if bar.low < current_ha_open:
                                await self.send_signal(symbol, 'LONG', current_close)
                                signal_sent_for_candle = True
                        elif prev2_color == 'green' and prev1_color == 'red':
                            if bar.high > current_ha_open:
                                await self.send_signal(symbol, 'SHORT', current_close)
                                signal_sent_for_candle = True

        except Exception as e:
            logger.error(f"Ошибка в мониторинге {symbol}: {e}")

    async def run(self):
        await self.load_symbols()
        logger.info(f"Мониторинг {len(self.all_symbols)} монет на {self.config['timeframe']}")
        tasks = [asyncio.create_task(self.monitor_symbol(symbol)) for symbol in self.all_symbols]
        await asyncio.gather(*tasks)

async def main():
    config = load_config()
    bot = HeikenAshiBot(config)
    await bot.run()

if __name__ == "__main__":
    asyncio.run(main())
