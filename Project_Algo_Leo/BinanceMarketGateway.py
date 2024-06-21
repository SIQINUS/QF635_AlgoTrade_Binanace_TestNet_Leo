"""
Develop an order gateway called BinanceFutureGateway that allows strategy to do the follow:
    - register callback to handle order book update - on_orderbook
    - register callback to handle execution update - on_execution
    - register callback to handle candle data update - on_candle_update
    - call connect() to start connection to Binance
    - place a post-only limit order

For simplicity, this gateway only need to support one symbol - BTCUSDT

"""

import asyncio
import json
import logging
import os
import time
from threading import Thread

import pandas as pd
import websockets
from binance import AsyncClient, BinanceSocketManager, Client
from binance.depthcache import FuturesDepthCacheManager
from dotenv import load_dotenv

from common.interface_book import VenueOrderBook, PriceLevel, OrderBook
from common.interface_order import OrderEvent, OrderStatus, ExecutionType, Side

logging.basicConfig(format='%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s', level=logging.INFO)


class BinanceFutureGateway:

    def __init__(self, symbol: str, api_key=None, api_secret=None, name='Binance', testnet=True):
        self._api_key = api_key
        self._api_secret = api_secret
        self._exchange_name = name
        self._symbol = symbol
        self.testnet = testnet

        # binance async client
        self._client = None
        self._async_client = None
        self._dcm = None  # depth cache, which implements the logic to manage a local order book
        self._dws = None  # depth async WebSocket session

        # depth cache
        self._depth_cache = None

        # this is a loop and dedicated thread to run all async concurrent tasks
        self._loop = asyncio.new_event_loop()
        self._loop_thread = Thread(target=self._run_async_tasks, daemon=True, name=name)

        # callbacks
        self._depth_callbacks = []
        self._execution_callbacks = []
        self._candle_callbacks = []

        self.orderbook = None
        # explicitly assign the returns
        self.trade_record = []

    def connect(self):
        logging.info('Initializing connection')

        self._loop.run_until_complete(self._reconnect_ws())

        logging.info("starting event loop thread")
        self._loop_thread.start()

        # synchronous client
        self._client = Client(self._api_key, self._api_secret, testnet=self.testnet)

    async def _reconnect_ws(self):
        logging.info("reconnecting websocket")
        self._async_client = await AsyncClient.create(self._api_key, self._api_secret, testnet=self.testnet)

    def _run_async_tasks(self):
        """ Run the following tasks concurrently in the current thread """
        self._loop.create_task(self._listen_depth_forever())
        self._loop.create_task(self._listen_execution_forever())
        self._loop.create_task(self._listen_candle_forever())
        self._loop.run_forever()

    async def _listen_depth_forever(self):
        logging.info("Subscribing to depth events")
        while True:
            if not self._dws:
                logging.info("depth socket not connected, reconnecting")
                self._dcm = FuturesDepthCacheManager(self._async_client, symbol=self._symbol)
                self._dws = await self._dcm.__aenter__()

            # wait for depth update
            try:
                self._depth_cache = await self._dws.recv()

                if self._depth_callbacks:
                    # notify callbacks
                    for _callback in self._depth_callbacks:
                        _callback(VenueOrderBook(self._exchange_name, self.get_order_book()))
            except Exception as e:
                logging.exception('encountered issue in depth processing')
                # reset socket and reconnect
                self._dws = None
                await self._reconnect_ws()

    async def _listen_candle_forever(self):
        logging.info("Subscribing to minute candles")
        bm = BinanceSocketManager(self._async_client)
        async with bm.kline_socket(self._symbol, interval='1m') as stream:
            while True:
                try:
                    candle = await stream.recv()
                    if candle:
                        data = candle['k']

                        if self._candle_callbacks:
                            for _callback in self._candle_callbacks:
                                _callback(data)

                except Exception as e:
                    logging.exception('encountered issue in candle processing')
                    await self._reconnect_ws()
                    await asyncio.sleep(58)  #enforce the candle will only wait 58s at least

    async def _listen_execution_forever(self):
        logging.info("Subscribing to user data events")
        _listen_key = await self._async_client.futures_stream_get_listen_key()
        if self.testnet:
            url = 'wss://stream.binancefuture.com/ws/' + _listen_key
        else:
            url = 'wss://fstream.binance.com/ws/' + _listen_key

        conn = websockets.connect(url)
        ws = await conn.__aenter__()
        while ws.open:
            _message = await ws.recv()
            _data = json.loads(_message)
            update_type = _data.get('e')

            if update_type == 'ORDER_TRADE_UPDATE':
                _trade_data = _data.get('o')
                _order_id = _trade_data.get('c')
                _symbol = _trade_data.get('s')
                _execution_type = _trade_data.get('x')
                _order_status = _trade_data.get('X')
                _side = _trade_data.get('S')
                _last_filled_price = float(_trade_data.get('L'))
                _last_filled_qty = float(_trade_data.get('l'))

                _order_event = OrderEvent(_symbol, _order_id, ExecutionType[_execution_type],
                                          OrderStatus[_order_status])
                _order_event.side = Side[_side]
                if _execution_type == 'TRADE':
                    _order_event.last_filled_price = _last_filled_price
                    _order_event.last_filled_quantity = _last_filled_qty
                    logging.info(_order_event)
                    self.trade_record.append(_order_event.__str__())

                if self._execution_callbacks:
                    for _callback in self._execution_callbacks:
                        _callback(_order_event)

    def get_order_book(self) -> OrderBook:
        if self._depth_cache:
            bids = [PriceLevel(price=p, size=s) for (p, s) in self._depth_cache.get_bids()[:5]]
            asks = [PriceLevel(price=p, size=s) for (p, s) in self._depth_cache.get_asks()[:5]]
            self.orderbook = OrderBook(timestamp=self._depth_cache.update_time, contract_name=self._symbol, bids=bids,
                                       asks=asks)
            return self.orderbook
        else:
            # logging.warning('Depth cache not available')
            pass

    def register_depth_callback(self, callback):
        self._depth_callbacks.append(callback)

    def register_execution_callback(self, callback):
        self._execution_callbacks.append(callback)

    def register_candle_callback(self, callback):
        self._candle_callbacks.append(callback)

# callback on order book update
def on_orderbook(order_book: VenueOrderBook):
    logging.info("Receive order book: {}".format(order_book))
    return order_book


# callback on execution update
def on_execution(order_event: OrderEvent):
    logging.info("Receive execution: {}".format(order_event))


# callback on candle data update
def on_candle_update(candle_data):
    logging.info("Receive candle data: {}".format(candle_data))


def parse_Klines(json_ob):
    # Initialize lists to store parsed data
    open_time = []
    close_time = []
    open_price = []
    high_price = []
    low_price = []
    close_price = []
    volume = []
    no_trades = []

    # Iterate over each entry in the JSON object
    for entry in json_ob:
        # Extract data from each entry and append to respective lists
        open_time.append(entry[0])
        close_time.append(entry[6])
        open_price.append(float(entry[1]))
        high_price.append(float(entry[2]))
        low_price.append(float(entry[3]))
        close_price.append(float(entry[4]))
        volume.append(float(entry[5]))
        no_trades.append(int(entry[8]))

    # Create DataFrame from parsed data
    df = pd.DataFrame({
        'open_time': open_time,
        'close_time': close_time,
        'open': open_price,
        'high': high_price,
        'low': low_price,
        'close': close_price,
        'volume': volume,
        'no_trades': no_trades
    })

    return df


