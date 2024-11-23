from psycopg2.extras import execute_values
from dotenv import load_dotenv
import psycopg2
import os

DATABASE_STRUCT_DICT = {
    "bronze_schema" :{"stock_prices_fact_table":{"columns_names": ['stock_symbol', 'current_price', 'change', 'percent_change', 'high_price_of_the_day', 'low_price_of_the_day', 'open_price_of_the_day', 'previous_close_price', 'Datetime'], "columns_data_types_list": ["String", "Float64", "Float64", "Float64", "Float64", "Float64", "Float64", "Float64", "String"]},
                      "company_information_dim_table":{"columns_names": ['symbol', 'price', 'beta', 'volAvg', 'mktCap', 'lastDiv', 'range', 'changes', 'companyName', 'currency', 'cik', 'isin', 'cusip', 'exchange', 'exchangeShortName', 'industry', 'website', 'description', 'ceo', 'sector', 'country', 'fullTimeEmployees', 'phone', 'address', 'city', 'state', 'zip', 'dcfDiff', 'dcf', 'image', 'ipoDate', 'defaultImage', 'isEtf', 'isActivelyTrading', 'isAdr', 'isFund', 'Datetime'], "columns_data_types_list": ["String", "Float64", "Float64", "Int64", "Int64", "Float64", "String", "Float64", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "Float64", "Float64", "String", "String", "Boolean", "Boolean", "Boolean", "Boolean", "Boolean", "String"]}},
    "silver_schema" :{"silver_stock_prices_fact":{"columns_names": ['stock_symbol', 'current_price', 'change', 'percent_change', 'high_price_of_the_day', 'low_price_of_the_day', 'open_price_of_the_day', 'previous_close_price', 'Datetime', 'Date', 'Day', 'Month', 'Year', 'Hour', 'Min', 'Sec'], "columns_data_types_list": ["String", "Float64", "Float64", "Float64", "Float64", "Float64", "Float64", "Float64", "String", "Date", "Int8", "Int8", "Int32", "Int8", "Int8", "Int8"]},
                      "silver_company_info_dim":{"columns_names": ['symbol', 'price', 'beta', 'volAvg', 'mktCap', 'lastDiv', 'range', 'changes', 'companyName', 'currency', 'cik', 'isin', 'cusip', 'exchange', 'exchangeShortName', 'industry', 'website', 'description', 'ceo', 'sector', 'country', 'fullTimeEmployees', 'phone', 'address', 'city', 'state', 'zip', 'dcfDiff', 'dcf', 'image', 'ipoDate', 'defaultImage', 'isEtf', 'isActivelyTrading', 'isAdr', 'isFund', 'Datetime', 'Date', 'Day', 'Month', 'Year', 'Hour', 'Min', 'Sec'], "columns_data_types_list": ["String", "Float64", "Float64", "Int64", "Int64", "Float64", "String", "Float64", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "Float64", "Float64", "String", "String", "Boolean", "Boolean", "Boolean", "Boolean", "Boolean", "String", "Date", "Int8", "Int8", "Int32", "Int8", "Int8", "Int8"]}},
    "gold_schema" :{"gold_table":{"columns_names": ['stock_symbol', 'companyName', 'current_price', 'percent_change', 'mktCap', 'sector', 'industry', 'country', 'Date', 'Day', 'Month', 'Year', 'Hour', 'Min', 'Sec'], "columns_data_types_list": ["String", "String", "Float64", "Float64", "Int64", "String", "String", "String", "Date", "Int8", "Int8", "Int32", "Int8", "Int8", "Int8"]},
                      "gold_company_information_dim_table":{"columns_names": ['symbol', 'price', 'beta', 'volAvg', 'mktCap', 'lastDiv', 'range', 'changes', 'companyName', 'currency', 'cik', 'isin', 'cusip', 'exchange', 'exchangeShortName', 'industry', 'website', 'description', 'ceo', 'sector', 'country', 'fullTimeEmployees', 'phone', 'address', 'city', 'state', 'zip', 'dcfDiff', 'dcf', 'image', 'ipoDate', 'defaultImage', 'isEtf', 'isActivelyTrading', 'isAdr', 'isFund', 'Datetime', 'Date', 'Day', 'Month', 'Year', 'Hour', 'Min', 'Sec'], "columns_data_types_list": ["String", "Float64", "Float64", "Int64", "Int64", "Float64", "String", "Float64", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "String", "Float64", "Float64", "String", "String", "Boolean", "Boolean", "Boolean", "Boolean", "Boolean", "String", "Date", "Int8", "Int8", "Int32", "Int8", "Int8", "Int8"]}}
}

DATA_TYPE_MAPPING_POLARS_TO_SQL = {
    "str": "TEXT",
    "f64": "DOUBLE PRECISION",
    "i64": "BIGINT",
    "bool": "BOOLEAN",
    "String": "TEXT",
    "Float64": "DOUBLE PRECISION",
    "Int8": "SMALLINT",
    "Int32": "INTEGER",
    "Boolean": "BOOLEAN",
    "Date": "DATE",
    "Int64": "BIGINT",
    "Int16": "SMALLINT"
}

class LoadDataProcess:
    def __init__(self):
        #env_path="./Data_Engineering_Pipeline/keys/postgresql.env"
        env_path="./keys/postgresql.env"
        current_directory = os.getcwd()
        print(current_directory)
        # full_path = os.path.join(current_directory, env_path)
        # normalized_path = os.path.normpath(full_path)
        load_dotenv(env_path, override=True)
        self.postgresql_config = {
            "host": os.getenv("POSTGRESQL_HOST"),
            "port": os.getenv("POSTGRESQL_PORT"),
            "dbname": os.getenv("POSTGRESQL_DB"),
            "user": os.getenv("POSTGRESQL_USER"),
            "password": os.getenv("POSTGRESQL_PASSWORD"),
        }
        self.db_struct = DATABASE_STRUCT_DICT
        self.type_mapping = DATA_TYPE_MAPPING_POLARS_TO_SQL

    def connect_to_db(self):
        try:
            conn = psycopg2.connect(**self.postgresql_config)
            return conn
        except Exception as e:
            raise Exception(f"Error connecting to PostgreSQL: {e}")

    def load_data_to_table(self, conn, schema, table_name, data):
        """
        Insert data into the specified table using execute_values
        """
        with conn.cursor() as cur:
            columns = data.columns
            table_columns = ", ".join(columns)
            query = f"INSERT INTO {schema}.{table_name} ({table_columns}) VALUES %s"
            data_values = [tuple(row) for row in data.to_numpy()]
            execute_values(cur, query, data_values)
            conn.commit()

    def process_table(self, schema, table_name, data, conn):
        """
        Process a table: Create and load data
        """
        table_info = self.db_struct[schema][table_name]
        self.load_data_to_table(conn, schema, table_name, data)

    def load_all_data(self, dataframes):
        """
        Load all DataFrames into their respective tables
        """
        conn = self.connect_to_db()
        try:
            for schema, tables in self.db_struct.items():
                for table_name in tables.keys():
                    if table_name in dataframes:
                        self.process_table(schema, table_name, dataframes[table_name], conn)
        finally:
            conn.close()

def usage():
    df_stock_prices, df_company_info = None, None
    silver_stock_prices, silver_company_info = None, None
    gold_table = None
    dataframes = {
        "stock_prices_fact_table": df_stock_prices,
        "company_information_dim_table": df_company_info,
        "silver_stock_prices_fact": silver_stock_prices,
        "silver_company_info_dim": silver_company_info,
        "gold_table": gold_table,
        "gold_company_information_dim_table": silver_company_info,
    }
    loader = LoadDataProcess()
    loader.load_all_data(dataframes)