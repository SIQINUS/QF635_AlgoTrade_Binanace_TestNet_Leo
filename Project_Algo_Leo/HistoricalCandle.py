import asyncio
import pandas as pd
from datetime import datetime
from binance.client import Client
import os
from dotenv import load_dotenv
import warnings

# Ignore all warnings
warnings.filterwarnings('ignore')
class RealTimeKlineTracker:
    def __init__(self, api_key, api_secret, symbol, interval='1m', window_size=100):
        self.client = Client(api_key, api_secret)
        self.symbol = symbol
        self.interval = interval
        self.window_size = window_size


    def fetch_latest_klines(self):

        self.df = pd.DataFrame(columns=['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time',
                                        'Quote asset volume', 'Number of trades', 'Taker buy base asset volume',
                                        'Taker buy quote asset volume', 'Ignore'])
        # Fetch the latest Klines
        klines = self.client.get_klines(symbol=self.symbol, interval=self.interval, limit=self.window_size)

        # Convert the fetched Klines to a DataFrame
        kline_data = [
            [
                datetime.utcfromtimestamp(kline[0] / 1000),
                float(kline[1]),
                float(kline[2]),
                float(kline[3]),
                float(kline[4]),
                float(kline[5]),
                datetime.utcfromtimestamp(kline[6] / 1000),
                float(kline[7]),
                int(kline[8]),
                float(kline[9]),
                float(kline[10]),
                kline[11]
            ]
            for kline in klines
        ]

        kline_df = pd.DataFrame(kline_data, columns=self.df.columns)
        # Concatenate the new Klines with the existing DataFrame and drop duplicates
        self.df = pd.concat([self.df, kline_df]).drop_duplicates().sort_index()
        # Maintain a rolling window of the last `window_size` Klines
        if len(self.df) > self.window_size:
            self.df = self.df.iloc[-self.window_size:]

        self.df.set_index('Open time', inplace=True)
        # Return the latest kline data frame
        return self.df

    def on_kline_update(self, df):
        # This method can be overridden to perform custom actions on Kline update
        print(df.tail(30))  # Print the last few rows of the DataFrame

    async def start(self, update_interval):
        while True:
            self.fetch_latest_klines()
            print(self.fetch_latest_klines().head())
            await asyncio.sleep(update_interval)

# Usage example
if __name__ == "__main__":
    dotenv_path = './vault/binance_keys'
    load_dotenv(dotenv_path=dotenv_path)
    api_key, api_secret = os.getenv('BINANCE_API_KEY'), os.getenv('BINANCE_API_SECRET')
    symbol = 'ETHUSDT'
    interval = Client.KLINE_INTERVAL_1MINUTE
    window_size = 120  # Define the rolling window size
    update_interval = 60  # Update interval in seconds

    tracker = RealTimeKlineTracker(api_key, api_secret, symbol, interval, window_size)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(tracker.start(update_interval))
    except KeyboardInterrupt:
        print("Process interrupted")
