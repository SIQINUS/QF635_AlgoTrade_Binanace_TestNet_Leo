# AccountGateWay.py
from datetime import datetime

import pandas as pd
import asyncio
import logging
from threading import Thread
from binance import AsyncClient, Client
from dotenv import load_dotenv
import os
import schedule

logging.basicConfig(format='%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s', level=logging.INFO)


class BinanceAccount:
    def __init__(self, api_key=None, api_secret=None, name='Accounts', testnet=True):
        self._api_key = api_key
        self._api_secret = api_secret
        self._exchange_name = name
        self.testnet = testnet

        # Binance clients
        self._client = None
        self._async_client = None

        # This is a loop and dedicated thread to run all async concurrent tasks
        self._loop = asyncio.new_event_loop()
        self._loop_thread = Thread(target=self._run_async_tasks, daemon=True, name=name)

        # Callbacks
        self._accountpnl_callbacks = []
        self._accountposition_callbacks = []
        self._openorders_callbacks = []
        self._trades_callbacks = []

        # Trades list for calculating realized pnl
        self._trades_list = []

        # Attributes to store the callback results
        self.realized_pnl = 0
        self.account_pnl_info = pd.DataFrame()
        self.account_position_info = pd.DataFrame()
        self.open_orders_info = pd.DataFrame()
        self.trades_info = pd.DataFrame()

    def connect(self):
        logging.info('Initializing connection')
        self._loop.run_until_complete(self._reconnect_ws())
        logging.info("Starting event loop thread")
        self._loop_thread.start()

        # Synchronous client
        self._client = Client(self._api_key, self._api_secret, testnet=self.testnet)

        # Job to extend listen key every 15 minutes
        schedule.every(15).minutes.do(self._extend_listen_key)

    async def _reconnect_ws(self):
        logging.info("Reconnecting websocket")
        self._async_client = await AsyncClient.create(self._api_key, self._api_secret, testnet=self.testnet)

    def _run_async_tasks(self):
        """Run the following tasks concurrently in the current thread"""
        asyncio.set_event_loop(self._loop)
        self._loop.create_task(self._listen_accountpnl_forever())
        self._loop.create_task(self._listen_accountposition_forever())
        self._loop.create_task(self._listen_openorders_forever())
        self._loop.create_task(self._listen_trades_forever())
        self._loop.run_forever()

    async def _listen_accountpnl_forever(self):
        logging.info("Subscribing to account PnL updates")
        while True:
            try:
                await self._fetch_and_notify_pnl()
            except Exception as e:
                logging.exception('Encountered issue in listening account PnL processing')

    async def _fetch_and_notify_pnl(self):
        """Fetch account PnL and notify callbacks"""
        try:
            account_info = await self._async_client.futures_account_balance()
            pnl_info = self._parse_pnl_info(account_info)
            self.account_pnl_info = pnl_info  # Store the fetched PnL info
            self.realized_pnl = pd.to_numeric(pnl_info['pnl']).sum()



            for callback in self._accountpnl_callbacks:
                callback(pnl_info)
        except Exception as e:
            logging.error(f"Error fetching account PnL data: {e}")

    async def _listen_accountposition_forever(self):
        logging.info("Subscribing to account position updates")
        while True:
            try:
                await self._fetch_and_notify_position()
                await asyncio.sleep(10)  # Fetch account position every 10 seconds
            except Exception as e:
                logging.exception('Encountered issue in listening account position processing')

    async def _fetch_and_notify_position(self):
        """Fetch account positions and notify callbacks"""
        try:
            position_info = await asyncio.wait_for(self._async_client.futures_position_information(), timeout=10.0)
            self.account_position_info = self._parse_position_info(position_info)

            for callback in self._accountposition_callbacks:
                callback(self.account_position_info)
        except Exception as e:
            logging.error(f"Error fetching position data: {e}")

    async def _listen_openorders_forever(self):
        logging.info("Subscribing to open orders updates")
        while True:
            try:
                await self._fetch_and_notify_openorders()
                await asyncio.sleep(10)  # Fetch open orders every 10 seconds
            except Exception as e:
                logging.exception('Encountered issue in listening open orders processing')

    async def _fetch_and_notify_openorders(self):
        """Fetch open orders and notify callbacks"""
        try:
            open_orders = await self._async_client.futures_get_open_orders()
            parsed_open_orders = self._parse_open_orders(open_orders)
            self.open_orders_info = parsed_open_orders  # Store the fetched open orders info

            for callback in self._openorders_callbacks:
                callback(parsed_open_orders)
        except Exception as e:
            logging.error(f"Error fetching open orders data: {e}")

    async def _listen_trades_forever(self):
        logging.info("Subscribing to trades updates")
        while True:
            try:
                await self._fetch_and_notify_trades()

                await asyncio.sleep(10)  # Fetch trades every 10 seconds
            except Exception as e:
                logging.exception('Encountered issue in listening trades processing')

    async def _fetch_and_notify_trades(self):
        """Fetch trades and notify callbacks"""
        try:
            trades = await self._async_client.futures_account_trades()
            self._trades_list.extend(trades)
            parsed_trades = self._parse_trades(self._trades_list)
            self.trades_info = parsed_trades  # Store the fetched trades info
              # Store the realized PnL

            for callback in self._trades_callbacks:
                callback(parsed_trades, self.realized_pnl)
        except Exception as e:
            logging.error(f"Error fetching trades data: {e}")




    def _parse_pnl_info(self, account_info):
        """Parse the account PnL information"""
        pnl_details = []
        for asset_info in account_info:
            asset = asset_info['asset']
            balance = asset_info['balance']
            crossUnpnl = asset_info.get('crossUnPnl', 'N/A')  # Handle missing keys
            pnl_details.append({'asset': asset, 'balance': balance, 'pnl': crossUnpnl})
        pnl_details = pd.DataFrame(pnl_details)
        return pnl_details

    def _parse_position_info(self, position_info):
        """Parse the account position information"""
        try:
            if not position_info:
                logging.info("Empty response received while parsing position data.")
                return pd.DataFrame()  # Return an empty DataFrame if the input is empty
            else:
                df = pd.DataFrame(position_info).convert_dtypes()
                cols_to_numeric = ['positionAmt', 'entryPrice', 'breakEvenPrice', 'markPrice',
                                   'unRealizedProfit', 'liquidationPrice', 'leverage', 'maxNotionalValue',
                                   'notional', 'adlQuantile']
                df[cols_to_numeric] = df[cols_to_numeric].apply(pd.to_numeric, errors='coerce')  # Coerce errors to NaN
                cols_toshow = ['symbol', 'positionAmt', 'notional', 'unRealizedProfit', 'entryPrice', 'markPrice']
                df['abs_notional'] = abs(df['notional'].values)
                df = df.sort_values(by='abs_notional', ascending=False)
                return df[cols_toshow]
        except Exception as e:
            logging.error(f"Error parsing position data: {e}")
            return pd.DataFrame()

    def _parse_open_orders(self, open_orders):
        """Parse the open orders information"""
        try:
            if not open_orders:
                logging.info("Empty response received while parsing open orders data.")
                return pd.DataFrame()  # Return an empty DataFrame if the input is empty
            else:
                df = pd.DataFrame(open_orders).convert_dtypes()
                cols_to_show = ['symbol', 'orderId', 'price', 'origQty', 'executedQty', 'status', 'type', 'side',
                                'time']
                df = df[cols_to_show]
                df['price'] = pd.to_numeric(df['price'], errors='coerce')
                df['origQty'] = pd.to_numeric(df['origQty'], errors='coerce')
                df['executedQty'] = pd.to_numeric(df['executedQty'], errors='coerce')
                df['time'] = pd.to_datetime(df['time'], unit='ms')
                return df
        except Exception as e:
            logging.error(f"Error parsing open orders data: {e}")
            return pd.DataFrame()

    def _parse_trades(self, trades):
        """Parse the trades information"""
        try:
            if not trades:
                logging.info("Empty response received while parsing trades data.")
                return pd.DataFrame()  # Return an empty DataFrame if the input is empty
            else:
                df = pd.DataFrame(trades).convert_dtypes()
                cols_to_show = ['symbol', 'orderId', 'price', 'qty', 'realizedPnl', 'side', 'time']
                df = df[cols_to_show]
                df['price'] = pd.to_numeric(df['price'], errors='coerce')
                df['qty'] = pd.to_numeric(df['qty'], errors='coerce')
                df['realizedPnl'] = pd.to_numeric(df['realizedPnl'], errors='coerce')
                df['time'] = pd.to_datetime(df['time'], unit='ms')
                return df
        except Exception as e:
            logging.error(f"Error parsing trades data: {e}")
            return pd.DataFrame()

    def _update_realized_pnl(self, trades_df):
        """Calculate the realized PnL from trades"""
        realized_pnl = trades_df['realizedPnl'].iloc[-1]
        return realized_pnl

    ### Add callback methods below
    def add_accountpnl_callback(self, callback):
        """Add a callback function to be called with account PnL updates"""
        self._accountpnl_callbacks.append(callback)

    def add_accountposition_callback(self, callback):
        """Add a callback function to be called with account position updates"""
        self._accountposition_callbacks.append(callback)

    def add_openorders_callback(self, callback):
        """Add a callback function to be called with open orders updates"""
        self._openorders_callbacks.append(callback)

    def add_trades_callback(self, callback):
        """Add a callback function to be called with trades updates"""
        self._trades_callbacks.append(callback)

    def _extend_listen_key(self):
        """Extend listen key (example method; actual implementation needed)"""
        pass
