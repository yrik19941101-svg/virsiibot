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
import os

CONFIG_FILE = "config.json"

def load_config():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BingXClient:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://open-api.bingx.com"
    
    def _generate_signature(self, params, timestamp):
        # Сортировка ключей
        sorted_keys = sorted(params.keys())
        query_string = '&'.join([f"{k}={params[k]}" for k in sorted_keys])
        payload = f"{timestamp}{query_string}"
        signature = hmac.new(self.api_secret.encode('utf-8'), payload.encode('utf-8'), hashlib.sha256).hexdigest()
        return signature

    def _request(self, endpoint, method='GET', params=None):
        timestamp = int(time.time() * 1000)
        if params is None:
            params = {}
        params['timestamp'] = timestamp
        signature = self._generate_signature(params, timestamp)
        headers = {
            'X-BX-APIKEY': self.api_key,
            'Content-Type': 'application/json'
        }
        url = f"{self.base_url}{endpoint}?{'&'.join([f'{k}={v}' for k, v in params.items()])}&signature={signature}"
        if method == 'GET':
            response = requests.get(url, headers=headers)
        else:
            response = requests.post(url, headers=headers, json=params)
        data = response.json()
        if data.get('code') != 0:
            logger.error(f"BingX API error: {data}")
            raise Exception(f"API error: {data}")
        return data.get('data')

    def get_klines(self, symbol, interval, limit=30):
        endpoint = "/openApi/swap/v2/market/klines"
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': limit
        }
        return self._request(endpoint, 'GET', params)

    def set_leverage(self, symbol, leverage):
        endpoint = "/openApi/swap/v2/trade/leverage"
        params = {
            'symbol': symbol,
            'leverage': leverage
        }
        return self._request(endpoint, 'POST', params)

    def place_order(self, symbol, side, quantity, price=None):
        endpoint = "/openApi/swap/v2/trade/order"
        params = {
            'symbol': symbol,
            'side': side.upper(),
            'type': 'MARKET',
            'quantity': quantity
        }
        if price:
            params['price'] = price
        return self._request(endpoint, 'POST', params)

    def cancel_order(self, order_id):
        endpoint = "/openApi/swap/v2/trade/cancel"
        params = {'orderId': order_id}
        return self._request(endpoint, 'POST', params)

    def get_position(self, symbol):
        endpoint = "/openApi/swap/v2/user/positions"
        params = {'symbol': symbol}
        return self._request(endpoint, 'GET', params)

    def close_position(self, symbol, side, quantity):
        # side - противоположный открытому: 'SELL' для LONG, 'BUY' для SHORT
        return self.place_order(symbol, side, quantity)

class HeikenAshiBot:
    def __init__(self, config):
        self.config = config
        self.client = BingXClient(config['api_key'], config['api_secret'])
        self.state = {}
        self.open_positions = {}

    async def load_symbols(self):
        self.all_symbols = [
            "TRIA-USDT",
            "EDGEX-USDT",
            "XPL-USDT",
            "PRL-USDT",
            "SIREN-USDT",
            "ENA-USDT",
            "AVAX-USDT",
            "1000PEPE-USDT"
        ]
        bot = Bot(token=self.config["telegram_token"])
        await bot.send_message(
            chat_id=self.config["telegram_chat_id"],
            text=f"✅ Бот запущен (автоторговля BingX)\n"
                 f"Таймфрейм: {self.config['timeframe']}\n"
                 f"Нач. сумма: ${self.config['trade_params']['default_trade_amount']}\n"
                 f"Мартингейл: до {self.config['trade_params']['max_martingale_steps']} шагов\n"
                 f"Мониторинг: {len(self.all_symbols)} монет",
            parse_mode='Markdown'
        )

    def get_leverage_for_symbol(self, symbol):
        per_coin = self.config.get('per_coin_settings', {})
        # конвертируем символ в формат как в per_coin_settings (с /USDT:USDT)
        symbol_key = symbol.replace('-', '/') + '/USDT:USDT'
        if symbol_key in per_coin:
            return per_coin[symbol_key]['leverage']
        return self.config['trade_params']['default_leverage']

    def get_trade_amount_for_symbol(self, symbol):
        state = self.state.get(symbol, {})
        step = state.get('martingale_step', 0)
        base = self.config['trade_params']['default_trade_amount']
        max_step = self.config['trade_params']['max_martingale_steps']
        if step >= max_step:
            return base
        return base * (2 ** step)

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
            df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover', 'openTime', 'closeTime'])
            df = df[['open', 'high', 'low', 'close', 'timestamp']].astype(float)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            logger.error(f"Ошибка получения данных для {symbol}: {e}")
            return None

    async def open_position(self, symbol, direction, price):
        try:
            leverage = self.get_leverage_for_symbol(symbol)
            self.client.set_leverage(symbol, leverage)
            trade_amount = self.get_trade_amount_for_symbol(symbol)
            # Расчёт количества: сумма * плечо / цена
            quantity = (trade_amount * leverage) / price
            quantity = round(quantity, 5)
            side = 'BUY' if direction == 'LONG' else 'SELL'
            order = self.client.place_order(symbol, side, quantity)
            logger.info(f"Ордер {direction} для {symbol}: {quantity} по {price}, сумма {trade_amount} USDT, плечо {leverage}")

            if direction == 'LONG':
                stop_price = price * (1 - 1/leverage)
                take_price = price * (1 + 1/leverage)
            else:
                stop_price = price * (1 + 1/leverage)
                take_price = price * (1 - 1/leverage)

            self.open_positions[symbol] = {
                'direction': direction,
                'entry_price': price,
                'quantity': quantity,
                'trade_amount': trade_amount,
                'leverage': leverage,
                'stop_price': stop_price,
                'take_price': take_price,
                'order_id': order.get('orderId'),
                'timestamp': datetime.now()
            }

            bot = Bot(token=self.config["telegram_token"])
            emoji = "🟢" if direction == 'LONG' else "🔴"
            direction_ru = "ПОКУПКА" if direction == 'LONG' else "ПРОДАЖА"
            message = (
                f"{emoji} **АВТО-СДЕЛКА {direction_ru}**\n\n"
                f"Монета: {symbol}\n"
                f"Цена входа: `{price:.5f}`\n"
                f"Сумма: {trade_amount} USDT\n"
                f"Плечо: {leverage}x\n"
                f"Стоп-лосс: `{stop_price:.5f}`\n"
                f"Тейк-профит: `{take_price:.5f}`\n"
                f"Время: {datetime.now().strftime('%H:%M:%S')}"
            )
            await bot.send_message(chat_id=self.config["telegram_chat_id"], text=message, parse_mode='Markdown')
            logger.info(f"Сделка {direction} для {symbol} открыта")
        except Exception as e:
            logger.error(f"Ошибка открытия позиции для {symbol}: {e}")

    async def monitor_positions(self):
        while True:
            for symbol, pos in list(self.open_positions.items()):
                try:
                    ticker = self.client.get_klines(symbol, '1m', 1)
                    if ticker:
                        current_price = float(ticker[0]['close'])
                        should_close = False
                        reason = None
                        if pos['direction'] == 'LONG':
                            if current_price <= pos['stop_price']:
                                should_close = True
                                reason = 'stop_loss'
                            elif current_price >= pos['take_price']:
                                should_close = True
                                reason = 'take_profit'
                        else:
                            if current_price >= pos['stop_price']:
                                should_close = True
                                reason = 'stop_loss'
                            elif current_price <= pos['take_price']:
                                should_close = True
                                reason = 'take_profit'

                        if should_close:
                            close_side = 'SELL' if pos['direction'] == 'LONG' else 'BUY'
                            self.client.close_position(symbol, close_side, pos['quantity'])
                            logger.info(f"Закрыта позиция {symbol} по {reason}, цена {current_price}")
                            if reason == 'stop_loss':
                                step = self.state.get(symbol, {}).get('martingale_step', 0) + 1
                                self.state.setdefault(symbol, {})['martingale_step'] = step
                                logger.info(f"{symbol}: стоп-лосс, шаг мартингейла = {step}")
                            else:
                                if symbol in self.state:
                                    self.state[symbol]['martingale_step'] = 0
                                logger.info(f"{symbol}: тейк-профит, мартингейл сброшен")
                            del self.open_positions[symbol]
                            bot = Bot(token=self.config["telegram_token"])
                            emoji = "🔴" if reason == 'stop_loss' else "🟢"
                            text = f"{emoji} **ПОЗИЦИЯ ЗАКРЫТА**\nМонета: {symbol}\nПричина: {reason}\nЦена закрытия: {current_price:.5f}\nВремя: {datetime.now().strftime('%H:%M:%S')}"
                            await bot.send_message(chat_id=self.config["telegram_chat_id"], text=text, parse_mode='Markdown')
                except Exception as e:
                    logger.error(f"Ошибка мониторинга позиции {symbol}: {e}")
            await asyncio.sleep(5)

    def check_signal(self, df):
        if len(df) < 10:
            return None
        # последние закрытые свечи
        prev2_color = df['ha_color'].iloc[-3]
        prev1_color = df['ha_color'].iloc[-2]
        current_candle = df.iloc[-1]
        current_ha_open = df['ha_open'].iloc[-1]

        if prev2_color == 'red' and prev1_color == 'green':
            if current_candle['low'] < current_ha_open:
                return 'LONG'
        elif prev2_color == 'green' and prev1_color == 'red':
            if current_candle['high'] > current_ha_open:
                return 'SHORT'
        return None

    async def process_symbol(self, symbol):
        if symbol in self.open_positions:
            return
        df = await self.get_market_data(symbol, limit=30)
        if df is None or len(df) < 10:
            return
        df = self.calculate_heiken_ashi(df)
        current_timestamp = df['timestamp'].iloc[-1]

        if symbol not in self.state:
            self.state[symbol] = {
                'last_candle_time': 0,
                'waiting_pullback': False,
                'signal_direction': None,
                'signal_sent': False,
                'martingale_step': 0
            }
        state = self.state[symbol]

        if current_timestamp != state['last_candle_time']:
            state['last_candle_time'] = current_timestamp
            state['signal_sent'] = False
            signal = self.check_signal(df)
            if signal == 'LONG':
                state['waiting_pullback'] = True
                state['signal_direction'] = 'LONG'
                logger.info(f"{symbol}: сигнал LONG, ждём отката вниз")
            elif signal == 'SHORT':
                state['waiting_pullback'] = True
                state['signal_direction'] = 'SHORT'
                logger.info(f"{symbol}: сигнал SHORT, ждём отката вверх")
            else:
                state['waiting_pullback'] = False
                state['signal_direction'] = None

        if state['waiting_pullback'] and not state['signal_sent']:
            current_candle = df.iloc[-1]
            current_ha_open = df['ha_open'].iloc[-1]
            if state['signal_direction'] == 'LONG' and current_candle['low'] < current_ha_open:
                await self.open_position(symbol, 'LONG', current_candle['close'])
                state['signal_sent'] = True
                state['waiting_pullback'] = False
            elif state['signal_direction'] == 'SHORT' and current_candle['high'] > current_ha_open:
                await self.open_position(symbol, 'SHORT', current_candle['close'])
                state['signal_sent'] = True
                state['waiting_pullback'] = False

    async def run(self):
        await self.load_symbols()
        logger.info(f"🚀 Мониторинг {len(self.all_symbols)} монет на {self.config['timeframe']}")
        asyncio.create_task(self.monitor_positions())
        while True:
            for symbol in self.all_symbols:
                try:
                    await self.process_symbol(symbol)
                except Exception as e:
                    logger.error(f"Ошибка {symbol}: {e}")
                await asyncio.sleep(0.5)
            await asyncio.sleep(60)

async def main():
    config = load_config()
    bot = HeikenAshiBot(config)
    try:
        await bot.run()
    finally:
        pass

if __name__ == "__main__":
    asyncio.run(main())
