import asyncio
import logging
from datetime import datetime
import ccxt.async_support as ccxt
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

class HeikenAshiBot:
    def __init__(self, config):
        self.config = config
        # Ключи из переменных окружения (безопасно) или из config (не рекомендуется)
        api_key = os.getenv('API_KEY') or config.get('api_key')
        api_secret = os.getenv('API_SECRET') or config.get('api_secret')
        if not api_key or not api_secret:
            logger.error("API ключи не найдены! Установите переменные окружения API_KEY и API_SECRET")
            raise ValueError("Missing API keys")
        self.exchange = getattr(ccxt, config["exchange"])({
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'},
            'apiKey': api_key,
            'secret': api_secret
        })
        self.state = {}
        self.open_positions = {}

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
            text=f"✅ Бот запущен (автоторговля)\n"
                 f"Биржа: {self.config['exchange']}\n"
                 f"Таймфрейм: {self.config['timeframe']}\n"
                 f"Нач. сумма: ${self.config['trade_params']['default_trade_amount']}\n"
                 f"Мартингейл: до {self.config['trade_params']['max_martingale_steps']} шагов\n"
                 f"Мониторинг: {len(self.all_symbols)} монет",
            parse_mode='Markdown'
        )

    def get_leverage_for_symbol(self, symbol):
        per_coin = self.config.get('per_coin_settings', {})
        if symbol in per_coin:
            return per_coin[symbol]['leverage']
        return self.config['trade_params']['default_leverage']

    def get_trade_amount_for_symbol(self, symbol):
        state = self.state.get(symbol, {})
        step = state.get('martingale_step', 0)
        base = self.config['trade_params']['default_trade_amount']
        max_step = self.config['trade_params']['max_martingale_steps']
        if step >= max_step:
            return base
        return base * (2 ** step)

    async def set_leverage(self, symbol, leverage):
        try:
            await self.exchange.set_leverage(leverage, symbol)
            logger.info(f"Установлено плечо {leverage} для {symbol}")
        except Exception as e:
            logger.error(f"Ошибка установки плеча для {symbol}: {e}")

    async def open_position(self, symbol, direction, price):
        try:
            leverage = self.get_leverage_for_symbol(symbol)
            await self.set_leverage(symbol, leverage)
            trade_amount = self.get_trade_amount_for_symbol(symbol)
            amount = (trade_amount * leverage) / price
            amount = round(amount, 5)
            side = 'buy' if direction == 'LONG' else 'sell'
            order = await self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=side,
                amount=amount,
                params={'reduceOnly': False}
            )
            logger.info(f"Ордер {direction} для {symbol}: {amount} по {price}, сумма {trade_amount} USDT, плечо {leverage}")

            if direction == 'LONG':
                stop_price = price * (1 - 1/leverage)
                take_price = price * (1 + 1/leverage)
            else:
                stop_price = price * (1 + 1/leverage)
                take_price = price * (1 - 1/leverage)

            self.open_positions[symbol] = {
                'direction': direction,
                'entry_price': price,
                'amount': amount,
                'trade_amount': trade_amount,
                'leverage': leverage,
                'stop_price': stop_price,
                'take_price': take_price,
                'order_id': order.get('id'),
                'timestamp': datetime.now()
            }

            bot = Bot(token=self.config["telegram_token"])
            emoji = "🟢" if direction == 'LONG' else "🔴"
            direction_ru = "ПОКУПКА" if direction == 'LONG' else "ПРОДАЖА"
            display_symbol = symbol.replace('/USDT:USDT', '')
            message = (
                f"{emoji} **АВТО-СДЕЛКА {direction_ru}**\n\n"
                f"Монета: {display_symbol}\n"
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
                    ticker = await self.exchange.fetch_ticker(symbol)
                    current_price = ticker['last']
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
                        side = 'sell' if pos['direction'] == 'LONG' else 'buy'
                        await self.exchange.create_order(
                            symbol=symbol,
                            type='market',
                            side=side,
                            amount=pos['amount']
                        )
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
                        text = f"{emoji} **ПОЗИЦИЯ ЗАКРЫТА**\nМонета: {symbol.replace('/USDT:USDT', '')}\nПричина: {reason}\nЦена закрытия: {current_price:.5f}\nВремя: {datetime.now().strftime('%H:%M:%S')}"
                        await bot.send_message(chat_id=self.config["telegram_chat_id"], text=text, parse_mode='Markdown')
                except Exception as e:
                    logger.error(f"Ошибка мониторинга позиции {symbol}: {e}")
            await asyncio.sleep(5)

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
        if symbol in self.open_positions:
            return
        df = await self.get_market_data(symbol, limit=30)
        if df is None or len(df) < 10:
            return
        ha_data = self.calculate_heiken_ashi(df)
        prev2_color = ha_data['ha_color'][-3]
        prev1_color = ha_data['ha_color'][-2]
        current_timestamp = df.iloc[-1]['timestamp']

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
            if prev2_color == 'red' and prev1_color == 'green':
                state['waiting_pullback'] = True
                state['signal_direction'] = 'LONG'
                logger.info(f"{symbol}: закрылась зелёная HA, ждём отката вниз для LONG")
            elif prev2_color == 'green' and prev1_color == 'red':
                state['waiting_pullback'] = True
                state['signal_direction'] = 'SHORT'
                logger.info(f"{symbol}: закрылась красная HA, ждём отката вверх для SHORT")
            else:
                state['waiting_pullback'] = False
                state['signal_direction'] = None

        if state['waiting_pullback'] and not state['signal_sent']:
            current_candle = df.iloc[-1]
            current_ha_open = ha_data['ha_open'][-1]
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
