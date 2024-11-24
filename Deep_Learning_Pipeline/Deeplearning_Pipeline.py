from sklearn.model_selection import train_test_split  
from sklearn.preprocessing import MinMaxScaler
from datetime import datetime, timedelta
from prefect import flow, task
from dotenv import load_dotenv
import torch.optim as optim
import torch.nn as nn
import polars as pl  
import pandas as pd
import numpy as np
import psycopg2 
import random
import torch
import onnx
import os

DATABASE_STRUCT_DICT = {
    "bronze_schema" :{"stock_prices_fact_table":{"columns_names": ['stock_symbol', 'current_price', 'change', 'percent_change', 'high_price_of_the_day', 'low_price_of_the_day', 'open_price_of_the_day', 'previous_close_price', 'Datetime'], "columns_data_types_list": ["String", "Float64", "Float64", "Float64", "Float64", "Float64", "Float64", "Float64", "String"]},
                      "company_information_dim_table":{"columns_names": ['symbol', 'price', 'beta', 'volAvg', 'mktCap', 'lastDiv', 'range', 'changes', 'companyName', 'currency', 'cik', 'isin', 'cusip', 'exchange', 'exchangeShortName', 'industry', 'website', 'description', 'ceo', 'sector', 'country', 'fullTimeEmployees', 'phone', 'address', 'city', 'state', 'zip', 'dcfDiff', 'dcf', 'image', 'ipoDate', 'defaultImage', 'isEtf', 'isActivelyTrading', 'isAdr', 'isFund', 'Datetime'], "columns_data_types_list": ["String", "Float64", "Float64", "Int64", "Int64", "Float64", "String", "Float64", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "Float64", "Float64", "String", "String", "Boolean", "Boolean", "Boolean", "Boolean", "Boolean", "String"]}},
    "silver_schema" :{"silver_stock_prices_fact":{"columns_names": ['stock_symbol', 'current_price', 'change', 'percent_change', 'high_price_of_the_day', 'low_price_of_the_day', 'open_price_of_the_day', 'previous_close_price', 'Datetime', 'Date', 'Day', 'Month', 'Year', 'Hour', 'Min', 'Sec'], "columns_data_types_list": ["String", "Float64", "Float64", "Float64", "Float64", "Float64", "Float64", "Float64", "String", "Date", "Int8", "Int8", "Int32", "Int8", "Int8", "Int8"]},
                      "silver_company_info_dim":{"columns_names": ['symbol', 'price', 'beta', 'volAvg', 'mktCap', 'lastDiv', 'range', 'changes', 'companyName', 'currency', 'cik', 'isin', 'cusip', 'exchange', 'exchangeShortName', 'industry', 'website', 'description', 'ceo', 'sector', 'country', 'fullTimeEmployees', 'phone', 'address', 'city', 'state', 'zip', 'dcfDiff', 'dcf', 'image', 'ipoDate', 'defaultImage', 'isEtf', 'isActivelyTrading', 'isAdr', 'isFund', 'Datetime', 'Date', 'Day', 'Month', 'Year', 'Hour', 'Min', 'Sec'], "columns_data_types_list": ["String", "Float64", "Float64", "Int64", "Int64", "Float64", "String", "Float64", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "Float64", "Float64", "String", "String", "Boolean", "Boolean", "Boolean", "Boolean", "Boolean", "String", "Date", "Int8", "Int8", "Int32", "Int8", "Int8", "Int8"]}},
    "gold_schema" :{"gold_table":{"columns_names": ['stock_symbol', 'companyName', 'current_price', 'percent_change', 'mktCap', 'sector', 'industry', 'country', 'Date', 'Day', 'Month', 'Year', 'Hour', 'Min', 'Sec'], "columns_data_types_list": ["String", "String", "Float64", "Float64", "Int64", "String", "String", "String", "Date", "Int8", "Int8", "Int32", "Int8", "Int8", "Int8"]},
                      "gold_company_information_dim_table":{"columns_names": ['symbol', 'price', 'beta', 'volAvg', 'mktCap', 'lastDiv', 'range', 'changes', 'companyName', 'currency', 'cik', 'isin', 'cusip', 'exchange', 'exchangeShortName', 'industry', 'website', 'description', 'ceo', 'sector', 'country', 'fullTimeEmployees', 'phone', 'address', 'city', 'state', 'zip', 'dcfDiff', 'dcf', 'image', 'ipoDate', 'defaultImage', 'isEtf', 'isActivelyTrading', 'isAdr', 'isFund', 'Datetime', 'Date', 'Day', 'Month', 'Year', 'Hour', 'Min', 'Sec'], "columns_data_types_list": ["String", "Float64", "Float64", "Int64", "Int64", "Float64", "String", "Float64", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "Float64", "Float64", "String", "String", "Boolean", "Boolean", "Boolean", "Boolean", "Boolean", "String", "Date", "Int8", "Int8", "Int32", "Int8", "Int8", "Int8"]}}
}

class StockPricePredictionModel:
    def __init__(self):
        self.scaler = None
        self.model = None
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        env_path="./keys/postgresql.env"
        #env_path="./Deeplearning_Pipeline/keys/postgresql.env"
        current_directory = os.getcwd()
        full_path = os.path.join(current_directory, env_path)
        normalized_path = os.path.normpath(full_path)
        load_dotenv(normalized_path, override=True)
        self.postgresql_config = {
            "host": os.getenv("POSTGRESQL_HOST"),
            "port": os.getenv("POSTGRESQL_PORT"),
            "dbname": os.getenv("POSTGRESQL_DB"),
            "user": os.getenv("POSTGRESQL_USER"),
            "password": os.getenv("POSTGRESQL_PASSWORD"),
        }

    def read_table_to_polars_dataframe(self):
        """
        Reads all data from a specified table in the given schema and imports it into a Polars DataFrame.
        Args:
            schema (str): The schema name.
            table_name (str): The table name.

        Returns:
            polars.DataFrame: The table data as a Polars DataFrame.
        """
        try:
            schema = "silver_schema"
            table = "silver_stock_prices_fact"
            table_metadata = DATABASE_STRUCT_DICT[schema][table]
            columns = table_metadata["columns_names"]
            column_types = table_metadata["columns_data_types_list"]
            
            polars_type_map = {
                "String": pl.Utf8,
                "Float64": pl.Float64,
                "Int8": pl.Int8,
                "Int32": pl.Int32,
                "Date": pl.Date,
                "Int64" : pl.Int64
            }
            
            schema_types = {col: polars_type_map[dtype] for col, dtype in zip(columns, column_types)}
            
            query = f"SELECT {', '.join(columns)} FROM {schema}.{table}"

            connection = psycopg2.connect(
                host=self.postgresql_config["host"],
                dbname=self.postgresql_config["dbname"],
                user=self.postgresql_config["user"],
                password=self.postgresql_config["password"],
                port=self.postgresql_config["port"]
            )
            
            cursor = connection.cursor()
            cursor.execute(query)
            data = cursor.fetchall()
            self.silver_stock_prices_fact = pl.DataFrame(data, schema=schema_types, orient="row")
            return self.silver_stock_prices_fact
        except Exception as e:
            print(f"Error: {e}")
            return None
        finally:
            try:
                connection.close()
            except Exception as e:
                return None

    def preprocess_data(self):
        df = self.silver_stock_prices_fact.with_columns([
            (pl.col("Datetime").str.strptime(pl.Datetime)).alias("Datetime")
        ])
        numeric_cols = ["current_price", "change", "percent_change", "high_price_of_the_day",
                        "low_price_of_the_day", "open_price_of_the_day", "previous_close_price"]
        df = df.select(numeric_cols)
        # Scaling
        self.scaler = MinMaxScaler()
        df_scaled = self.scaler.fit_transform(df.to_pandas())
        return df_scaled

    def create_sequences(self, data, sequence_length=60):
        X, y = [], []
        for i in range(len(data) - sequence_length):
            X.append(data[i:i + sequence_length])
            y.append(data[i + sequence_length, 0])  # Predicting the current_price
        return np.array(X), np.array(y)

    class LSTMModel(nn.Module):
        def __init__(self, input_size, hidden_size, num_layers, output_size):
            super().__init__()
            self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
            self.fc = nn.Linear(hidden_size, output_size)

        def forward(self, x):
            lstm_out, _ = self.lstm(x)
            lstm_out = lstm_out[:, -1, :]  # Take the last output of LSTM
            return self.fc(lstm_out)

    def build_model(self, input_size, hidden_size=64, num_layers=2, output_size=1):
        self.model = self.LSTMModel(input_size, hidden_size, num_layers, output_size).to(self.device)

    def train_model(self, X_train, y_train, X_val, y_val, epochs=10, batch_size=32, learning_rate=0.001):
        criterion = nn.MSELoss()
        optimizer = optim.Adam(self.model.parameters(), lr=learning_rate)

        X_train_tensor = torch.tensor(X_train, dtype=torch.float32).to(self.device)
        y_train_tensor = torch.tensor(y_train, dtype=torch.float32).to(self.device)
        X_val_tensor = torch.tensor(X_val, dtype=torch.float32).to(self.device)
        y_val_tensor = torch.tensor(y_val, dtype=torch.float32).to(self.device)

        train_dataset = torch.utils.data.TensorDataset(X_train_tensor, y_train_tensor)
        train_loader = torch.utils.data.DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

        for epoch in range(epochs):
            self.model.train()
            for batch_X, batch_y in train_loader:
                optimizer.zero_grad()
                outputs = self.model(batch_X)
                loss = criterion(outputs, batch_y.unsqueeze(1))
                loss.backward()
                optimizer.step()

            self.model.eval()
            with torch.no_grad():
                val_outputs = self.model(X_val_tensor)
                val_loss = criterion(val_outputs, y_val_tensor.unsqueeze(1))
            print(f"Epoch {epoch + 1}/{epochs}, Loss: {loss.item()}, Val Loss: {val_loss.item()}")

    def save_model_to_onnx(self, filepath="./models/model.onnx", input_size=7, sequence_length=60):
        dummy_input = torch.randn(1, sequence_length, input_size).to(self.device)
        try:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
        except OSError as error:
            print("Directory './models/model.onnx' can not be created")   
        torch.onnx.export(self.model, dummy_input, filepath, export_params=True, opset_version=11)
        print(f"Model saved as {filepath}")

    def process(self):
        self.read_table_to_polars_dataframe()
        data_scaled = self.preprocess_data()

        X, y = self.create_sequences(data_scaled)
        X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

        self.build_model(input_size=X_train.shape[2])
        self.train_model(X_train, y_train, X_val, y_val)
        self.save_model_to_onnx()

# if __name__ == "__main__":
#     stock_model = StockPricePredictionModel()
#     #stock_model.process()
#     stock_model.read_table_to_polars_dataframe()
#     data_scaled = stock_model.preprocess_data()

#     X, y = stock_model.create_sequences(data_scaled)
#     X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

#     stock_model.build_model(input_size=X_train.shape[2])
#     stock_model.train_model(X_train, y_train, X_val, y_val)
#     stock_model.save_model_to_onnx()

@task
def initialize_model():
    print("Initializing the Stock Price Prediction Model...")
    return StockPricePredictionModel()

@task
def read_data(stock_model: StockPricePredictionModel):
    print("Reading data from the database...")
    stock_model.read_table_to_polars_dataframe()
    return stock_model

@task
def preprocess_data(stock_model: StockPricePredictionModel):
    print("Preprocessing data...")
    data_scaled = stock_model.preprocess_data()
    return stock_model, data_scaled

@task
def create_sequences(stock_model: StockPricePredictionModel, data_scaled):
    print("Creating sequences...")
    X, y = stock_model.create_sequences(data_scaled)
    return stock_model, X, y

@task
def split_data(X, y):
    print("Splitting data into train and validation sets...")
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)
    return X_train, X_val, y_train, y_val

@task
def build_model(stock_model : StockPricePredictionModel, input_size):
    print("Building the LSTM model...")
    stock_model.build_model(input_size=input_size)
    return stock_model

@task
def train_model(stock_model : StockPricePredictionModel, X_train, y_train, X_val, y_val):
    print("Training the LSTM model...")
    stock_model.train_model(X_train, y_train, X_val, y_val)
    return stock_model

@task
def save_model(stock_model : StockPricePredictionModel):
    print("Saving the model to ONNX format...")
    stock_model.save_model_to_onnx()
    print("Model saved successfully.")

@flow(log_prints=True)
def deeplearning_development_workflow_execution():
    stock_model = initialize_model()
    stock_model = read_data(stock_model)
    stock_model, data_scaled = preprocess_data(stock_model)
    stock_model, X, y = create_sequences(stock_model, data_scaled)
    X_train, X_val, y_train, y_val = split_data(X, y)
    stock_model = build_model(stock_model, input_size=X_train.shape[2])
    stock_model = train_model(stock_model, X_train, y_train, X_val, y_val)
    save_model(stock_model)
    print("Workflow execution completed successfully.")

if __name__ == "__main__":
    deeplearning_development_workflow_execution.serve(
        name="deeplearning_development_workflow_execution",
        cron="0 23 * * *"
    )