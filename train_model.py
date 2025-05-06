
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from helpers.file_data import read_parquet_file, save_model
class TrainModel:
    def select_features(self, df):
        features = ["sma_200", "rsi", "macd_histogram", "macd_signal"]
        X = df[features]
        Y = df["label"]

        return X, Y
    
    def train_model(self, X, Y):
        X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.3, random_state=42)

        rf_model = RandomForestClassifier(n_estimators=100, random_state=42)
        rf_model.fit(X_train, Y_train)

    def train(self):
        df = read_parquet_file("btc_usdt_5m_features.parquet")
        X, Y = self.select_features(df)
        rf_model = self.train_model(X, Y)
        save_model(rf_model, "random_forest_model_5m")