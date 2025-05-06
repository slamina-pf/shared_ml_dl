import pandas as pd
import ta
import pyarrow.parquet as pq
from helpers.file_data import save_parquet_file, read_parquet_file

class FeatureEngineering:

    def date_time_features(self, df):
        
        df['year'] = df['timestamp'].dt.year
        df['month'] = df['timestamp'].dt.month
        df['day'] = df['timestamp'].dt.day
        df['dayofweek'] = df['timestamp'].dt.dayofweek
        df['is_weekend'] = df['dayofweek'].isin([5, 6]).astype(int)

        return df
    
    def technical_indicators(self, df):

        df["sma_50"] = ta.trend.sma_indicator(df["close"], window=50)
        df["sma_200"] = ta.trend.sma_indicator(df["close"], window=200)
        df["ema_10"] = ta.trend.ema_indicator(df["close"], window=10)
        df["rsi"] = ta.momentum.RSIIndicator(df["close"], window=14).rsi()
        df["macd"] = ta.trend.macd(df["close"])
        df["macd_signal"] = ta.trend.macd_signal(df["close"])
        df["macd_histogram"] = ta.trend.macd_diff(df["close"])

        return df
    
    def target_label(self, df):
        # Signal is '1' (buy) when:
        # - SMA 50 > SMA 200 (trend up)
        # - RSI > 55 (stronger momentum confirmation)
        # - MACD > MACD Signal (bullish crossover)
        buy_signal = (
            (df["sma_50"] > df["sma_200"]) &
            (df["rsi"] > 55) &
            (df["macd"] > df["macd_signal"])
        )

        # Signal is '-1' (sell) when:
        # - SMA 50 < SMA 200 (trend down)
        # - RSI < 45 (bearish momentum)
        # - MACD < MACD Signal (bearish crossover)
        sell_signal = (
            (df["sma_50"] < df["sma_200"]) &
            (df["rsi"] < 45) &
            (df["macd"] < df["macd_signal"])
        )

        df["label"] = 0  # Default: Hold / No action
        df.loc[buy_signal, "label"] = 1
        df.loc[sell_signal, "label"] = -1

        return df
    
    def drop_na(self, df):
        # Drop rows with NaN values
        df.dropna(inplace=True)
        return df
    
    def feature_engineering(self):
        df = read_parquet_file("btc_usdt_5m_cleaned.parquet")

        df = self.date_time_features(df)
        df = self.technical_indicators(df)
        df = self.target_label(df)
        df = self.drop_na(df)
        save_parquet_file(df, "btc_usdt_5m_features.parquet")