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
        self.exchange = getattr(ccxt, config["exchange"])({
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'}
        })
        # Состояние для каждой монеты
        self.state = {}

    async def load_symbols(self):
        self.all_symbols = [
            "TRIA/USDT:USDT",
            "EDGEX/USDT:USDT",
            "XPL/USDT:USDT",
            "PRL/USDT:USDT",
            "SIREN/USDT:USDT",
            "ENA/USDT:USDT",
            "AVAX/USDT:USDT",
            "1000PEPE/USDT:USDT"
        ]
        bot = Bot(token=self.config["telegram_token"])
        await bot.send_message(
            chat_id=self.config["telegram_chat_id"],
            text=f"✅ Бот запущен (фьючерсы, 2h таймфрейм)\n"
                 f"Биржа: {self.config['exchange']}\n"
                 f"Таймфрейм: {self.config['timeframe']}\n"
                 f"Мониторинг: {len(self.all_symbols)} монет\n"
                 f"Стратегия: закрытие свечи HA → откат → один сигнал",
            parse_mode='Markdown'
        )

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

    async def process_symbol(self, symbol):
        df = await self.get_market_data(symbol, limit=30)
        if df is None or len(df) < 10:
            return
        ha_data = self.calculate_heiken_ashi(df)

        # Индексы:
        # -1: текущая (незакрытая) свеча
        # -2: предыдущая закрытая свеча
        # -3: позапрошлая закрытая
        prev2_color = ha_data['ha_color'][-3]
        prev1_color = ha_data['ha_color'][-2]
        current_timestamp = df.iloc[-1]['timestamp']

        # Инициализация состояния
        if symbol not in self.state:
            self.state[symbol] = {
                'last_candle_time': 0,
                'waiting_pullback': False,
                'signal_direction': None,
                'signal_sent': False,      # флаг, что сигнал уже отправлен на текущем откате
                'last_signal_time': 0,
                'last_signal_price': 0
            }

        state = self.state[symbol]

        # --- Обработка закрытия свечи (появление нового бара) ---
        if current_timestamp != state['last_candle_time']:
            state['last_candle_time'] = current_timestamp
            state['signal_sent'] = False   # новый бар — можно отправлять сигнал заново

            # Проверяем смену цвета на закрывшейся свече (prev1_color относительно prev2_color)
            if prev2_color == 'red' and prev1_color == 'green':
                # Сигнал на LONG: предыдущая красная, закрылась зелёная
                state['waiting_pullback'] = True
                state['signal_direction'] = 'LONG'
                state['signal_candle_close'] = df.iloc[-2]['close']
                logger.info(f"{symbol}: закрылась зелёная HA, ждём отката вниз для LONG")
            elif prev2_color == 'green' and prev1_color == 'red':
                # Сигнал на SHORT
                state['waiting_pullback'] = True
                state['signal_direction'] = 'SHORT'
                state['signal_candle_close'] = df.iloc[-2]['close']
                logger.info(f"{symbol}: закрылась красная HA, ждём отката вверх для SHORT")
            else:
                # Нет сигнала — сбрасываем ожидание
                state['waiting_pullback'] = False
                state['signal_direction'] = None

        # --- Ожидание отката на текущей свече ---
        if state['waiting_pullback'] and not state['signal_sent']:
            current_candle = df.iloc[-1]
            current_ha_open = ha_data['ha_open'][-1]

            if state['signal_direction'] == 'LONG':
                # Откат вниз: текущая свеча должна опуститься ниже HA_Open
                if current_candle['low'] < current_ha_open:
                    await self.send_signal(symbol, 'LONG', current_candle['close'])
                    state['signal_sent'] = True
                    state['waiting_pullback'] = False
                    state['last_signal_time'] = datetime.now()
                    state['last_signal_price'] = current_candle['close']
            elif state['signal_direction'] == 'SHORT':
                if current_candle['high'] > current_ha_open:
                    await self.send_signal(symbol, 'SHORT', current_candle['close'])
                    state['signal_sent'] = True
                    state['waiting_pullback'] = False
                    state['last_signal_time'] = datetime.now()
                    state['last_signal_price'] = current_candle['close']

        # Защита от слишком частых сигналов (например, если по какой-то причине сбросится флаг)
        # Проверяем, что прошло больше 2 часов с последнего сигнала по той же монете
        if 'last_signal_time' in state:
            time_since_last = (datetime.now() - state['last_signal_time']).total_seconds()
            if time_since_last < 7200 and state.get('signal_sent', False):
                # Если не прошло 2 часа и сигнал уже был, игнорируем
                pass

    async def send_signal(self, symbol, direction, price):
        bot = Bot(token=self.config["telegram_token"])
        emoji = "🟢" if direction == 'LONG' else "🔴"
        direction_ru = "ПОКУПКА" if direction == 'LONG' else "ПРОДАЖА"
        display_symbol = symbol.replace('/USDT:USDT', '')
        message = (
            f"{emoji} **СИГНАЛ НА {direction_ru}**\n\n"
            f"Монета: {display_symbol}\n"
            f"Таймфрейм: {self.config['timeframe']}\n"
            f"Цена входа: `{price:.5f}`\n\n"
            f"Время: {datetime.now().strftime('%H:%M:%S')}"
        )
        await bot.send_message(chat_id=self.config["telegram_chat_id"], text=message, parse_mode='Markdown')
        logger.info(f"✅ СИГНАЛ {direction} для {symbol} по цене {price}")

    async def run(self):
        await self.load_symbols()
        logger.info(f"🚀 Мониторинг {len(self.all_symbols)} монет на {self.config['timeframe']}")

        while True:
            for symbol in self.all_symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.error(f"Ошибка при обработке {symbol}: {e}")
                await asyncio.sleep(0.5)  # задержка между монетами
            await asyncio.sleep(60)  # пауза между полными циклами

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
