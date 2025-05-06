import pyarrow.parquet as pq
import pandas as pd
import os
from helpers.constants import TEMP_DATA_GENERAL_PATH

class DataCleaning:
    """ def __init__(self, data):
        self.data = data """

    def dropna(self, df):
        df.dropna(inplace=True)
        return df
    
    def drop_duplicates(self, df):
        df.drop_duplicates(inplace=True)
        return df

    def handle_outliers(self, df):
        # Check if the DataFrame contains numeric columns
        numeric_columns = df.select_dtypes(include=['number']).columns
        
        if len(numeric_columns) == 0:
            print("No numeric columns available for outlier handling.")
            return df
        
        # Handle outliers using the IQR method and impute with the mean
        for column in numeric_columns:
            # Calculate Q1 (25th percentile) and Q3 (75th percentile)
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            
            # Define lower and upper bounds for outliers
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            # Identify outliers
            outliers = (df[column] < lower_bound) | (df[column] > upper_bound)
            
            # Impute outliers with the column mean
            mean_value = df[column].mean()
            df[column] = df[column].where(~outliers, mean_value)
        
        return df

    def save(self, df):
        # Save the cleaned DataFrame to a new Parquet file

        os.makedirs(f'{TEMP_DATA_GENERAL_PATH}', exist_ok=True)
        df = pd.DataFrame(df, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df.to_parquet(f'{TEMP_DATA_GENERAL_PATH}/btc_usdt_5m_cleaned.parquet', index=False)


    def clean_data(self):
        table = pq.read_table(f'{TEMP_DATA_GENERAL_PATH}/btc_usdt_5m.parquet')
        df = table.to_pandas()
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms')

        df = self.dropna(df)
        df = self.drop_duplicates(df)
        df = self.handle_outliers(df)
        self.save(df)

