import asyncio
import json
import logging
from datetime import datetime

import websockets
import ccxt.async_support as ccxt
import pandas as pd
from telegram import Bot

CONFIG_FILE = "config.json"

def load_config():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RealtimeHeikenAshiBot:
    def __init__(self, config):
        self.config = config
        self.telegram_bot = Bot(token=config["telegram_token"])
        self.chat_id = config["telegram_chat_id"]
        self.timeframe = config["timeframe"]
        self.symbols = config["symbols"]

        self.symbols_state = {}
        for symbol in self.symbols:
            self.symbols_state[symbol] = {
                "ha_data": [],
                "pending_signal": False,
                "last_candle_ts": None
            }

        self.exchange = getattr(ccxt, config["exchange"])({'enableRateLimit': True})

    async def load_initial_data(self):
        logger.info("Загрузка начальных исторических данных...")
        for symbol in self.symbols:
            try:
                ohlcv = await self.exchange.fetch_ohlcv(symbol, self.timeframe, limit=100)
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                ha_df = self.calculate_heiken_ashi(df)
                self.symbols_state[symbol]['ha_data'] = ha_df.to_dict('records')
                logger.info(f"Загружены исторические данные для {symbol}")
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Ошибка загрузки истории для {symbol}: {e}")
        await self.send_startup_message()

    @staticmethod
    def calculate_heiken_ashi(df: pd.DataFrame) -> pd.DataFrame:
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

    async def send_startup_message(self):
        await self.telegram_bot.send_message(
            chat_id=self.chat_id,
            text=f"✅ Бот запущен (реальное время, WebSocket)\n"
                 f"Таймфрейм: {self.timeframe}\n"
                 f"Мониторинг: {len(self.symbols)} монет\n\n"
                 f"_Стратегия: смена цвета HA → откат → сигнал_",
            parse_mode='Markdown'
        )

    async def send_signal(self, symbol, direction, price, timestamp):
        emoji = "🟢" if direction == 'LONG' else "🔴"
        direction_ru = "ПОКУПКА" if direction == 'LONG' else "ПРОДАЖА"
        message = (
            f"{emoji} **СИГНАЛ НА {direction_ru}**\n\n"
            f"Монета: {symbol}\n"
            f"Таймфрейм: {self.timeframe}\n"
            f"Цена входа: `{price:.5f}`\n\n"
            f"Время: {timestamp}"
        )
        await self.telegram_bot.send_message(chat_id=self.chat_id, text=message, parse_mode='Markdown')
        logger.info(f"СИГНАЛ {direction} для {symbol} по цене {price}")

    def update_ha_and_check_signal(self, symbol, new_candle):
        state = self.symbols_state[symbol]
        ha_data = state['ha_data']
        ha_data.append(new_candle)
        if len(ha_data) > 30:
            ha_data.pop(0)

        df = pd.DataFrame(ha_data[-5:])
        ha_df = self.calculate_heiken_ashi(df)

        if len(ha_df) < 3:
            return None, None

        prev2_color = ha_df['ha_color'].iloc[-3]
        prev1_color = ha_df['ha_color'].iloc[-2]
        current_candle = ha_df.iloc[-1]

        if prev2_color == 'red' and prev1_color == 'green':
            if current_candle['low'] < current_candle['ha_open']:
                return 'LONG', current_candle['close']

        if prev2_color == 'green' and prev1_color == 'red':
            if current_candle['high'] > current_candle['ha_open']:
                return 'SHORT', current_candle['close']

        return None, None

    async def websocket_handler(self):
        uri = "wss://stream.bybit.com/v5/public/spot"
        while True:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                    for symbol in self.symbols:
                        topic = f"kline.{self.timeframe}.{symbol}"
                        subscribe_msg = {"op": "subscribe", "args": [topic]}
                        await ws.send(json.dumps(subscribe_msg))
                        logger.info(f"Подписка на {topic}")
                        await asyncio.sleep(0.1)

                    async for message in ws:
                        data = json.loads(message)
                        if 'data' not in data:
                            continue

                        kline_data = data['data'][0]
                        symbol = kline_data['symbol']
                        start_time = kline_data['start']
                        open_price = float(kline_data['open'])
                        high_price = float(kline_data['high'])
                        low_price = float(kline_data['low'])
                        close_price = float(kline_data['close'])

                        current_ts = pd.to_datetime(start_time, unit='ms')
                        state = self.symbols_state.get(symbol)
                        if not state:
                            continue

                        if state['last_candle_ts'] != current_ts:
                            state['last_candle_ts'] = current_ts
                            state['pending_signal'] = False

                        if state['pending_signal']:
                            continue

                        new_candle = {
                            'timestamp': current_ts,
                            'open': open_price,
                            'high': high_price,
                            'low': low_price,
                            'close': close_price,
                            'volume': 0
                        }

                        direction, entry_price = self.update_ha_and_check_signal(symbol, new_candle)
                        if direction:
                            await self.send_signal(symbol, direction, entry_price, datetime.now().strftime('%H:%M:%S'))
                            state['pending_signal'] = True

            except Exception as e:
                logger.error(f"WebSocket ошибка: {e}. Переподключение через 5 секунд...")
                await asyncio.sleep(5)

    async def close(self):
        await self.exchange.close()

    async def run(self):
        await self.load_initial_data()
        await self.websocket_handler()

async def main():
    config = load_config()
    bot = RealtimeHeikenAshiBot(config)
    try:
        await bot.run()
    finally:
        await bot.close()

if __name__ == "__main__":
    asyncio.run(main())
