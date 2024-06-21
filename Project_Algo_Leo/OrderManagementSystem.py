# OrderManagementSystem.py
import asyncio
import logging
import os
from binance import AsyncClient, Client
import numpy as np
from datetime import datetime
from dotenv import load_dotenv


class OrderManagementSystem:
    def __init__(self, symbol, api_key, api_secret, testnet=True):
        self.symbol = symbol
        self.client = Client(api_key, api_secret, testnet=testnet)
        self.async_client = None
        self.tick_size = 0.01  # Default tick size
        self.price_precision = 8  # Default precision
        self.quantity_precision = 8  # Default quantity precision
        self.load_exchange_info()

    async def async_init(self, api_key, api_secret, testnet):
        self.async_client = await AsyncClient.create(api_key, api_secret, testnet=testnet)

    def load_exchange_info(self):
        """ Load precision and tick size for the symbol from Binance API. """
        info = self.client.futures_exchange_info()
        for symbol_info in info['symbols']:
            if symbol_info['symbol'] == self.symbol:
                for _filter in symbol_info['filters']:
                    if _filter['filterType'] == 'PRICE_FILTER':
                        self.tick_size = float(_filter['tickSize'])
                        self.price_precision = int(-np.log10(float(_filter['tickSize'])))
                    if _filter['filterType'] == 'LOT_SIZE':
                        self.quantity_precision = int(-np.log10(float(_filter['stepSize'])))

    def check_order_book(self):
        order_book = self.client.get_order_book(symbol=self.symbol)
        best_bid = float(order_book['bids'][0][0])
        best_ask = float(order_book['asks'][0][0])
        return best_bid, best_ask

    async def place_limit_order(self, symbol, price, quantity, side):
        order_params = {
            'symbol': symbol,
            'side': side.upper(),
            'type': 'LIMIT',
            'timeInForce': 'GTC',  # Changed from 'GTX' to 'GTC' for a regular limit order
            'price': round(price, 2),
            'quantity': round(quantity, 1)
        }
        try:
            order = self.client.futures_create_order(**order_params)
            logging.info(f"Placed limit order: {order}")
            await self.log_trade_info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " " + str(order), "TradesInfo.txt")
            return order
        except Exception as e:
            logging.exception(f"Failed to place limit order: {e}")

    async def place_market_order(self, _symbol, quantity, side):
        order_params = {
            'symbol': _symbol,
            'side': side.upper(),
            'type': 'MARKET',
            'quantity': quantity
        }
        try:
            order = await self.async_client.futures_create_order(**order_params)
            logging.info(f"Placed market order: {order}")
            return order
        except Exception as e:
            logging.exception(f"Failed to place market order: {e}")

    async def cancel_all_open_orders(self, symbol):
        try:
            open_orders = self.client.futures_get_open_orders(symbol=symbol)
            if not open_orders:
                logging.info("No Open orders currently")
                return False
            else:
                for order in open_orders:
                    response = await self.cancel_order(symbol, order['orderId'])
                    await self.log_trade_info(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " " + str(response),
                                              "TradesInfo.txt")
                return True  # Indicates that orders were canceled
        except Exception as e:
            logging.exception(f"Failed to fetch or cancel open orders for symbol {symbol}: {e}")
            return False  # Indicates failure

    async def cancel_order(self, _symbol, _orderId):
        order_params = {'symbol': _symbol, 'orderId': _orderId}
        try:
            response = self.client.futures_cancel_order(**order_params)
            logging.info(f"Cancelled order {_orderId} for symbol {_symbol}: {response}")
            return response
        except Exception as e:
            logging.exception(f"Failed to cancel order {_orderId} for symbol {_symbol}: {e}")

    async def place_limit_order_with_cancellation(self, _symbol, price, quantity, side):
        if await self.cancel_all_open_orders(_symbol):
            logging.info(f"All open orders for {_symbol} have been canceled.")
            response = await self.place_limit_order(_symbol, price, quantity, side)
            await asyncio.sleep(5)
            return response
        else:
            response = await self.place_limit_order(_symbol, price, quantity, side)
            await asyncio.sleep(5)
            return response

    async def place_market_order_with_cancellation(self, _symbol, quantity, side):
        if await self.cancel_all_open_orders(_symbol):
            logging.info(f"All open orders for {_symbol} have been canceled.")
            response = await self.place_market_order(_symbol, quantity, side)
            await asyncio.sleep(1)
            return response
        else:
            response = await self.place_market_order(_symbol, quantity, side)
            await asyncio.sleep(1)
            return response

    async def log_trade_info(self, data, filename):
        try:
            with open(filename, 'a') as file:  # 'a' mode opens the file for appending
                file.write(data + '\n')  # Add a newline character for separation
                await asyncio.sleep(10)
        except Exception as e:
            print(f"An error occurred: {e}")

#
# # Example usage
# if __name__ == "__main__":
#     logging.basicConfig(format='%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s',
#                         level=logging.INFO)
#     dotenv_path = './vault/binance_keys'
#     load_dotenv(dotenv_path=dotenv_path)
#     api_key = os.getenv('BINANCE_API_KEY')
#     api_secret = os.getenv('BINANCE_API_SECRET')
#     _symbol = 'ETHUSDT'
#     oms = OrderManagementSystem(_symbol, api_key, api_secret)
#     asyncio.run(oms.place_limit_order(_symbol, 3560, 0.1, 'BUY'))
