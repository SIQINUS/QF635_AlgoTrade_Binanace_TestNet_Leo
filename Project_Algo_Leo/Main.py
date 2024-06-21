import asyncio
import logging
import os
import threading
from datetime import datetime
from threading import Thread
import csv
from datetime import datetime
from dotenv import load_dotenv
from AccountGateWay import BinanceAccount
from BinanceMarketGateway import BinanceFutureGateway
from HistoricalCandle import RealTimeKlineTracker
from SignalGenerator import SignalGenerator
from common.interface_book import VenueOrderBook
from OrderManagementSystem import OrderManagementSystem

logging.basicConfig(format='%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s', level=logging.INFO)

global_position = 0.0
position_lock = threading.Lock()


def on_orderbook(order_book: VenueOrderBook):
    return order_book


def account_pnl_callback(pnl_info):
    pass


def open_orders_callback(open_orders_info):
    pass


def trades_callback(trades_info, realized_pnl):
    logging.info(f"Realized PnL: {realized_pnl}")


def account_position_callback(position_info):
    global global_position
    try:
        eth_position = position_info[position_info['symbol'] == _symbol]
        if not eth_position.empty:
            with position_lock:
                global_position = float(eth_position['positionAmt'].iloc[0])
            logging.info(f"Updated global position for {_symbol}: {global_position}")
        else:
            logging.info(f"No position information for {_symbol}")
    except Exception as e:
        logging.error(f"Error updating position: {e}")


async def trading_logic(tracker, binance_account, order_manager, binance_gateway, max_limit_single_pair=1.5):
    global global_position
    while True:
        try:
            df_candle = tracker.fetch_latest_klines()
            signal_generator = SignalGenerator(df_candle)
            signal_generator.evaluate_signals()
            side, quantity = signal_generator.generate_signal()

            with position_lock:
                current_position = global_position
                logging.info(f'Realized P&L {binance_account.realized_pnl}, Current position: {current_position}')

            if quantity:
                while binance_gateway.get_order_book():

                    with position_lock:
                        current_position = global_position

                    if current_position >= max_limit_single_pair:
                        _price = binance_gateway.get_order_book().get_best_ask()
                        if side == 'SELL':
                            logging.info(f'Exceed Limit, only reduce order {side} is allowed')
                            response = await order_manager.place_limit_order_with_cancellation(_symbol, _price,
                                                                                               quantity,
                                                                                               side)
                        else:
                            logging.warning(f'Exceed Limit, proactive reduce Long')
                            _new_qty = round(abs(current_position - max_limit_single_pair) + 0.3, 1)
                            response = await order_manager.place_limit_order_with_cancellation(_symbol, _price - 1,
                                                                                               _new_qty, 'SELL')
                        if response['status'] == 'FILLED':
                            logging.info(f"Exceeding the limit {max_limit_single_pair}. revised back to limit")
                            break

                    elif current_position <= -max_limit_single_pair:
                        _price = binance_gateway.get_order_book().get_best_bid()
                        if side == 'BUY':
                            logging.info(f'Exceed Limit, only reduce order {side} is allowed')
                            response = await order_manager.place_limit_order_with_cancellation(_symbol, _price,
                                                                                               quantity,
                                                                                               side)
                        else:
                            logging.warning(f'Exceed Limit, proactive reduce Short')
                            _new_qty = round(abs(current_position + max_limit_single_pair) + 0.3, 1)
                            response = await order_manager.place_limit_order_with_cancellation(_symbol, _price + 1,
                                                                                               _new_qty, 'BUY')
                        if response['status'] == 'FILLED':
                            logging.info(f"Exceeding the limit {max_limit_single_pair}. revised back to limit")
                            break

                    else:
                        logging.info(f"Pre-trade check done. Generated signal: Side={side}, Quantity={quantity}")
                        if side == 'BUY':
                            price = binance_gateway.get_order_book().get_best_bid()
                            response = await order_manager.place_limit_order_with_cancellation(_symbol, price + 1,
                                                                                               quantity,
                                                                                               side)
                            if response['status'] == 'FILLED':
                                break

                        elif side == 'SELL':
                            price = binance_gateway.get_order_book().get_best_ask()
                            response = await order_manager.place_limit_order_with_cancellation(_symbol, price - 1,
                                                                                               quantity,
                                                                                               side)
                            if response['status'] == 'FILLED':
                                break

        except Exception as e:
            logging.error(f"Error in trading logic: {e}")


class RiskManagement:
    def __init__(self, symbol, binance_account, binance_gateway, order_manager,
                 max_direct_loss=0.05, max_notional_portfolio=10000, max_draw_down=0.4):
        self.symbol = symbol
        self.take_profit_pnl = 50
        self.max_pnl = 0
        self.max_notional_portfolio = max_notional_portfolio
        self.max_direct_loss = max_direct_loss
        self.max_draw_down = max_draw_down
        self.risk_flag = True
        self.binance_account = binance_account
        self.binance_gateway = binance_gateway
        self.order_manager = order_manager
        self.risk_thread = Thread(target=self._run_risk_manager_thread, daemon=True, name="RiskManagement")
        logging.info("Risk Manager Initialized")

    def connect(self):
        logging.info("Risk Manager Connected")
        self.risk_thread.start()

    def _run_risk_manager_thread(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.create_task(self._run_risk_manager())
        self.loop.run_forever()

    async def _run_risk_manager(self):
        while True:
            await self.apply_risk_management_open_position_and_portfolio()
            await asyncio.sleep(5)

    async def apply_risk_management_open_position_and_portfolio(self):
        """max limit 3, open loss 4%, max draw down 70%"""
        global global_position
        with position_lock:
            current_position = global_position
        logging.info(f"Risk Manager => Using global position: {current_position}")
        try:
            info_symbol = self.binance_account.account_position_info.loc[
                self.binance_account.account_position_info.symbol == self.symbol]
            _position = current_position
            _entryPrice = float(info_symbol.entryPrice.iloc[0])

            if self.binance_gateway.get_order_book():
                _bid_price = self.binance_gateway.get_order_book().get_best_bid()
                _ask_price = self.binance_gateway.get_order_book().get_best_ask()
                if current_position > 0:
                    _currentProfit = current_position * (-_entryPrice + _bid_price)
                else:
                    #short position
                    _currentProfit = current_position * (_entryPrice - _ask_price)

                """Logging info and to csv"""
                logging.info(
                    f"Risk Manager => Position: {_position:.2f} | Current Profit: {_currentProfit} | Entry Price: {_entryPrice:.2f},"
                    f"Realized Pnl: {binance_account.realized_pnl}")

                tmp_data = [datetime.now(), _position, _currentProfit, _entryPrice, binance_account.realized_pnl]
                try:
                    with open('AccountInfo.csv', 'a', newline='') as file:  # 'a' mode opens the file for appending
                        writer = csv.writer(file)
                        writer.writerow(tmp_data)
                except Exception as e:
                    print(f"An error occurred: {e}")

                if abs(_position * _entryPrice) > self.max_notional_portfolio:
                    logging.warning(f'Limit Exceed. Liquidating half the position')
                    await self.liquidate_position(liquid_ratio=0.5)

                if self.binance_gateway.get_order_book():
                    if _currentProfit < -self.max_direct_loss * _entryPrice * abs(_position) and abs(_position) > 0.5:
                        logging.warning(
                            f"Risk Manager => Open Profit is less than -4% Liquidating all the Position")
                        await self.liquidate_position()

                    elif _currentProfit > self.take_profit_pnl:
                        logging.warning(f"Current Profit is greater than Max Profit. Booking half the profit")
                        await self.liquidate_position(liquid_ratio=0.5)

                    if self.max_pnl <= self.binance_account.realized_pnl:
                        self.max_pnl = self.binance_account.realized_pnl

                    if self.binance_account.realized_pnl > 100:
                        if self.binance_account.realized_pnl / self.max_pnl <= (1 - self.max_draw_down):
                            logging.warning('Max Drawdown exceed 0.3, liquidating half position')
                            await self.liquidate_position(liquid_ratio=0.5)

                    if self.binance_gateway.get_order_book():  #loss 15%
                        if _currentProfit < -self.max_direct_loss * 3 * _entryPrice * abs(_position):
                            logging.warning(
                                f"Risk Manager => Open Profit is less than -8% Liquidating all at market ")
                            await self.liquidate_position(type=False, aggressive_level= 0.5)


        except Exception as e:
            logging.error(f"Error applying risk management: {e}")

    async def liquidate_position(self, liquid_ratio=1.0, type=False, aggressive_level = 0.25):
        global global_position
        logging.warning(f"Risk Manager => Liquidating position: {global_position}")
        self.risk_flag = False
        try:
            if type:
                with position_lock:
                    _position = global_position
                _side = 'SELL' if _position >= 0 else 'BUY'
                await self.order_manager.place_market_order_with_cancellation(self.symbol, abs(_position), _side)

            else:
                while True:
                    with position_lock:
                        _position = global_position
                    if abs(_position) <= 0.5:
                        logging.info(f"Position sufficiently reduced to {_position:.2f}")
                        self.risk_flag = True
                        break
                    else:
                        if _position > 0 and self.binance_gateway.get_order_book():
                            _market_square_price = self.binance_gateway.get_order_book().get_best_bid()
                            _join_price = self.binance_gateway.get_order_book().get_best_ask()
                            _limit_square_price = _join_price - aggressive_level*(-_market_square_price + _join_price)
                            _side = 'SELL'
                            response = await self.order_manager.place_limit_order_with_cancellation(self.symbol,
                                                                                                    _limit_square_price,
                                                                                                    abs(_position) * liquid_ratio,
                                                                                                    _side)
                            logging.warning(f"Risk Manager => Liquidate position {_position} at {_limit_square_price:.2f}")
                            logging.warning(f"Order response: {response}")

                            if response['status'] == 'FILLED':
                                with position_lock:
                                    _position = global_position
                                logging.warning(f"New Position after Liquidation {_position:.2f}")
                                self.risk_flag = True
                                break

                        elif _position <= 0 and self.binance_gateway.get_order_book():
                            _market_square_price = self.binance_gateway.get_order_book().get_best_ask()
                            _join_price = self.binance_gateway.get_order_book().get_best_bid()
                            _limit_square_price = _join_price + aggressive_level*(_market_square_price - _join_price)
                            _side = 'BUY'
                            response = await self.order_manager.place_limit_order_with_cancellation(self.symbol,
                                                                                                    _limit_square_price,
                                                                                                    abs(_position) * liquid_ratio,
                                                                                                    _side)
                            logging.warning(f"Risk Manager => Liquidate position {_position} at {_limit_square_price:.2f}")
                            logging.warning(f"Order response: {response}")

                            if response['status'] == 'FILLED':
                                with position_lock:
                                    _position = global_position
                                logging.warning(f"New Position after Liquidation {_position:.2f}")
                                self.risk_flag = True
                                break
                        else:
                            pass
                    await asyncio.sleep(1)

        except Exception as e:
            logging.error(f"Error in liquidating position: {e}")


if __name__ == '__main__':
    dotenv_path = './vault/binance_keys'
    load_dotenv(dotenv_path=dotenv_path)
    api_key = os.getenv('BINANCE_API_KEY')
    api_secret = os.getenv('BINANCE_API_SECRET')
    _symbol = 'ETHUSDT'
    # max_notional_portfolio = 10000
    # max_single_pair = 3
    tracker = RealTimeKlineTracker(api_key, api_secret, symbol=_symbol)
    binance_gateway = BinanceFutureGateway(_symbol, api_key, api_secret)
    binance_gateway.register_depth_callback(on_orderbook)

    binance_gateway.connect()

    binance_account = BinanceAccount(api_key, api_secret)
    binance_account.add_accountpnl_callback(account_pnl_callback)
    binance_account.add_accountposition_callback(account_position_callback)
    binance_account.add_openorders_callback(open_orders_callback)
    binance_account.add_trades_callback(trades_callback)
    binance_account.connect()

    order_manager = OrderManagementSystem(_symbol, api_key, api_secret)

    risk_manager = RiskManagement(_symbol, binance_account, binance_gateway, order_manager)
    risk_manager.connect()

    asyncio.run(trading_logic(tracker, binance_account, order_manager, binance_gateway))
