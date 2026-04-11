import asyncio
import logging
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
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
        self.open_positions = set()
        self.pos_data = {}
        self.all_symbols = []
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
        all_swap = [symbol for symbol, market in self.exchange.markets.items()
                    if market['swap'] and market['quote'] == 'USDT']
        self.all_symbols = all_swap
        logger.info(f"Загружено {len(self.all_symbols)} фьючерсных пар")
        logger.info(f"Таймфрейм: {self.config['timeframe']}")

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

    def add_indicators(self, df):
        # Bollinger Bands (20,2)
        df['bb_mid'] = df['close'].rolling(window=20).mean()
        bb_std = df['close'].rolling(window=20).std()
        df['bb_upper'] = df['bb_mid'] + bb_std * 2
        df['bb_lower'] = df['bb_mid'] - bb_std * 2
        # RSI (14)
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        # Средний объём за 10 свечей
        df['avg_volume'] = df['volume'].rolling(window=10).mean()
        return df

    async def check_filters(self, symbol, direction, price, volume):
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, self.config['timeframe'], limit=50)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df = self.add_indicators(df)
            last = df.iloc[-1]
            if direction == 'LONG':
                # Цена ниже средней полосы (не обязательно касание нижней)
                if last['close'] > last['bb_mid']:
                    return False
                if last['rsi'] > 50:
                    return False
                if volume < last['avg_volume']:
                    return False
            else:  # SHORT
                if last['close'] < last['bb_mid']:
                    return False
                if last['rsi'] < 50:
                    return False
                if volume < last['avg_volume']:
                    return False
            return True
        except Exception as e:
            logger.error(f"Ошибка фильтров {symbol}: {e}")
            return True  # при ошибке пропускаем фильтр

    async def open_position(self, symbol, direction, price, volume):
        if len(self.open_positions) >= self.config['max_positions']:
            logger.warning(f"Лимит позиций ({self.config['max_positions']}) достигнут")
            return

        trade_amount = self.config['trade_params']['fixed_trade_amount']
        leverage = self.config['trade_params']['default_leverage']
        side = 'LONG' if direction == 'LONG' else 'SHORT'
        order_side = 'buy' if direction == 'LONG' else 'sell'

        quantity = (trade_amount * leverage) / price
        quantity = round(quantity, 5)
        if quantity <= 0:
            logger.error(f"Неверное количество {symbol}")
            return

        if not await self.check_filters(symbol, direction, price, volume):
            logger.info(f"{symbol}: фильтры не пройдены, вход отменён")
            return

        try:
            sl_percent = self.config['trade_params']['sl_percent']
            tp_percent = self.config['trade_params']['tp_percent']
            if direction == 'LONG':
                stop_price = round(price * (1 - (1/leverage) * sl_percent), 5)
                take_price = round(price * (1 + (1/leverage) * tp_percent), 5)
            else:
                stop_price = round(price * (1 + (1/leverage) * sl_percent), 5)
                take_price = round(price * (1 - (1/leverage) * tp_percent), 5)

            await self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=order_side,
                amount=quantity,
                params={'positionSide': side}
            )
            logger.info(f"🟢 ОТКРЫТА {direction} {symbol}: {quantity} по {price}, сумма {trade_amount} USDT")
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
                   f"SL: {stop_price:.5f}\nTP: {take_price:.5f}\nБаланс: {balance:.2f} USDT")
            await self.send_telegram(msg)
        except Exception as e:
            logger.error(f"Ошибка открытия {symbol}: {e}")

    async def close_position(self, symbol, reason, current_price):
        pos = self.pos_data.get(symbol)
        if not pos or pos.get('closed'):
            return
        try:
            close_side = 'sell' if pos['direction'] == 'LONG' else 'buy'
            side = 'LONG' if pos['direction'] == 'LONG' else 'SHORT'
            await self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=close_side,
                amount=pos['quantity'],
                params={'positionSide': side}
            )
            logger.info(f"🔴 ЗАКРЫТА {symbol} по {reason}, цена {current_price}")
            self.pos_data[symbol]['closed'] = True
            balance = await self.get_balance()
            emoji = "🔴" if reason == 'stop_loss' else "🟢"
            msg = f"{emoji} СДЕЛКА ЗАКРЫТА\nМонета: {symbol}\nПричина: {reason}\nЦена: {current_price:.5f}\nБаланс: {balance:.2f} USDT"
            await self.send_telegram(msg)
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
                            logger.info(f"{symbol}: трейлинг-стоп активирован")
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
                state['signal_volume'] = sig_candle['volume']
                logger.info(f"{symbol}: сигнал LONG, ждём отката вниз")
            elif prev2 == 'green' and prev1 == 'red':
                state['waiting_for_pullback'] = True
                state['signal_direction'] = 'SHORT'
                state['signal_candle_close'] = sig_candle['close']
                state['signal_volume'] = sig_candle['volume']
                logger.info(f"{symbol}: сигнал SHORT, ждём отката вверх")
            else:
                state['waiting_for_pullback'] = False

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
        await self.send_telegram(
            f"🚀 БОТ ЗАПУЩЕН (АГРЕССИВНЫЙ РЕЖИМ)\n"
            f"Таймфрейм: {self.config['timeframe']}\n"
            f"Сумма сделки: {self.config['trade_params']['fixed_trade_amount']} USDT\n"
            f"SL/TP: 50%\n"
            f"Макс. позиций: {self.config['max_positions']}\n"
            f"Баланс: {balance:.2f} USDT\n"
            f"Цель: $4/час"
        )
        while True:
            for symbol in self.all_symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.error(f"Ошибка {symbol}: {e}")
                await asyncio.sleep(0.5)
            await asyncio.sleep(30)  # 30 секунд между циклами для 5-минутного ТФ

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
