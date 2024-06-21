
import logging

from queue import Queue


class SignalGenerator:
    def __init__(self, candles):
        self.candles = candles
        self.order_queue = Queue()
        self.side = None
        self.quantity = 0
        self.price = None

    def calculate_rsi(self, period=14):
        delta = self.candles['Close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)

        avg_gain = gain.rolling(window=period, min_periods=1).mean()
        avg_loss = loss.rolling(window=period, min_periods=1).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        logging.info(f'The RSI index is {rsi.iloc[-1]}')
        return rsi.iloc[-1]

    def signal_rsi(self):  # MEAN REVISION
        rsi = self.calculate_rsi()
        if rsi >= 75:
            return 'SELL'
        elif rsi <= 25:
            return 'BUY'

    def signal_simple_ma(self):  # MOMENTUM
        self.candles['SMA20'] = self.candles['Close'].rolling(window=20).mean()

        if self.candles['Close'].iloc[-1] > self.candles['SMA20'].iloc[-1]:
            return 'BUY'
        else:
            return 'SELL'

    def calculate_vwap(self):
        self.candles['Cumulative_Volume'] = self.candles['Volume'].cumsum()
        self.candles['Cumulative_Price_Volume'] = (self.candles['Close'] * self.candles['Volume']).cumsum()
        self.candles['VWAP'] = self.candles['Cumulative_Price_Volume'] / self.candles['Cumulative_Volume']
        self.candles['VWAP_SMA5'] = self.candles['Close'].rolling(window=5).mean()

    def signal_vwap(self):  # MEAN REVISION
        self.calculate_vwap()

        current_price = self.candles['Close'].iloc[-1]
        current_vwap = self.candles['VWAP_SMA5'].iloc[-1]

        logging.info(f'Current price: {current_price}, VWAP: {current_vwap}')

        if current_price < current_vwap:
            logging.info('VWAP Strategy: BUY signal generated')
            return 'BUY'
        elif current_price > current_vwap:
            logging.info('VWAP Strategy: SELL signal generated')
            return 'SELL'

    def evaluate_signals(self):
        if len(self.candles) < 60:
            return

        # Prioritize RSI signal
        rsi_signal = self.signal_rsi()
        if rsi_signal:
            logging.info('RSI signal prioritized')
            self.order_queue.put((rsi_signal, 0.1))
            return

        # Evaluate Simple MA and VWAP signals
        ma_signal = self.signal_simple_ma()
        vwap_signal = self.signal_vwap()

        # If MA and VWAP signals contradict, follow MA
        if ma_signal and vwap_signal:
            if ma_signal != vwap_signal:
                logging.info('MA and VWAP signals contradict, following Vwap signal')
                self.order_queue.put((vwap_signal, 0.1))
            else:
                logging.info('MA and VWAP signals agree, doubling position')
                self.order_queue.put((ma_signal, 0.2))
        elif ma_signal:
            logging.info('Simple MA signal prioritized after RSI')
            self.order_queue.put((ma_signal, 0.1))
        elif vwap_signal:
            logging.info('VWAP signal prioritized after RSI and Simple MA')
            self.order_queue.put((vwap_signal, 0.1))

    def generate_signal(self):
        self.side, self.quantity = self.order_queue.get()
        return self.side, self.quantity



#
# if __name__ == '__main__':
#     logging.basicConfig(format='%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s',
#                         level=logging.INFO)
#     dotenv_path = './vault/binance_keys'
#     load_dotenv(dotenv_path=dotenv_path)
#     api_key = os.getenv('BINANCE_API_KEY')
#     api_secret = os.getenv('BINANCE_API_SECRET')
#     _symbol = 'ETHUSDT'
#
#     # Step One, get the historical klines, past 60mins minute klines
#     traker = RealTimeKlineTracker(api_key, api_secret, symbol=_symbol)
#     df_candle = traker.fetch_latest_klines()
#
#     # Step2: Generate the Signal, side, quantity
#     signal_generator = SignalGenerator(df_candle)
#     signal_generator.evaluate_signals()
#
#     # Retrieve generated signals from the order queue
#     while not signal_generator.order_queue.empty():
#         side, quantity = signal_generator.generate_signal()
#         print(f"Generated signal: Side={side}, Quantity={quantity}")
