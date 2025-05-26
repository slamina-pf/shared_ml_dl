import pandas as pd
import time
from datetime import datetime, timedelta, timezone
from helpers.connections import BINANCE_NORMAL_CONNECTION
from helpers.constants import TEMP_DATA_GENERAL_PATH
import pandas as pd
import os

class DataCollector:
    def __init__(
            self, 
            symbol: str = "BTC/USDT", 
            timeframe: str = "5m", 
            days: int = 365,
            limit: int = 1000, 
            data_storage_name: str = "btc_usdt_5m",
        ):
        self.symbol = symbol
        self.timeframe = timeframe
        self.days = days
        self.limit = limit
        self.data_storage_name = data_storage_name
        self.end_date = datetime.now(timezone.utc)
        self.start_date = datetime.now(timezone.utc) - timedelta(days=self.days)

    def to_milliseconds(self, dt):
        return int(dt.timestamp() * 1000)

    def get_data_from_binance(self):

        since = self.to_milliseconds(self.start_date)
        end_timestamp = self.to_milliseconds(self.end_date)

        all_candles = []

        while since < end_timestamp:
            
            candles = BINANCE_NORMAL_CONNECTION.fetch_ohlcv(self.symbol, self.timeframe, since, self.limit)

            if not candles:
                break

            all_candles.extend(candles)

            since = candles[-1][0] + 1

            time.sleep(BINANCE_NORMAL_CONNECTION.rateLimit / 1000)
        
        return all_candles
    
    def save_to_parquet(self, df):
        os.makedirs(f'{TEMP_DATA_GENERAL_PATH}', exist_ok=True)
        df.to_parquet(f'{TEMP_DATA_GENERAL_PATH}/{self.data_storage_name}.parquet', index=False)

    def collect_data(self):
        data = self.get_data_from_binance()
        df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms')

        return df
