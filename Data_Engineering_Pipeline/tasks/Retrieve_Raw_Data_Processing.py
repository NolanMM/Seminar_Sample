from dotenv import load_dotenv
from datetime import datetime
import polars as pl
import requests
import finnhub
import os

class RetrieveRawDataProcess:
    #def __init__(self, env_path="./Data_Engineering_Pipeline/keys/finhub.env"):
    def __init__(self, env_path="./keys/finhub.env"):
        # current_directory = os.getcwd()
        # full_path = os.path.join(current_directory, env_path)
        # normalized_path = os.path.normpath(full_path)
        load_dotenv(env_path, override=True)
        self.stock_list = []
        self.symbol_prices_dict = {}
        self.symbol_company_info_dict = {}
        stock_list_file_path = os.getenv("STOCK_LIST_FILE_PATH")
        # stock_list_file_path_full = os.path.join(current_directory, stock_list_file_path)
        self.stock_list_file_path = stock_list_file_path # os.path.normpath(stock_list_file_path_full)
        self.set_up_finhub_api()
        self.set_up_financial_modeling_prep_api()

    def set_up_finhub_api(self):
        self.finhub_api_key = os.getenv("FINHUB_API")
        self.finnhub_client = finnhub.Client(api_key=self.finhub_api_key)
        
    def set_up_financial_modeling_prep_api(self):
        self.financialmodelingprep_api_key = os.getenv("FINANCIALMODELINGPREP_API")
        self.profile_route = os.getenv("FINANCIALMODELINGPREP_PROFILE_ROUTE")

    def retrieve_data_process_run(self):
        try:
            self.load_stock_list()
            self.get_stock_prices()
            self.get_company_information()
            return self.to_dataframe()
        except Exception as e:
            return None, None

    def load_stock_list(self):
        """Load the list of stock symbols from the specified file."""
        with open(self.stock_list_file_path, 'r') as file:
            self.stock_list = [line.strip() for line in file]

    def get_stock_prices(self):
        """Retrieve current stock price data for each symbol in the stock list."""
        for symbol in self.stock_list:
            try:
                price_data = self.finnhub_client.quote(symbol)
                self.symbol_prices_dict[symbol] = price_data
            except Exception as e:
                print(f"Error retrieving price data for {symbol}: {e}")

    def get_company_information(self):
        """Fetch company information for each stock symbol using the Financial Modeling Prep API."""
        for symbol in self.stock_list:
            try:
                url = f"{self.profile_route}{symbol}?apikey={self.financialmodelingprep_api_key}"
                response = requests.get(url)
                response.raise_for_status()
                company_info = response.json()
                self.symbol_company_info_dict[symbol] = company_info
            except Exception as e:
                print(f"Error retrieving company information for {symbol}: {e}")

    def to_dataframe(self):
        """Convert the collected data to Polars DataFrames and add a Datetime column."""
        current_time = datetime.now().isoformat()

        # Stock prices DataFrame
        stock_rows = [
            {
                "stock_symbol": symbol,
                "current_price": data.get("c"),
                "change": data.get("d"),
                "percent_change": data.get("dp"),
                "high_price_of_the_day": data.get("h"),
                "low_price_of_the_day": data.get("l"),
                "open_price_of_the_day": data.get("o"),
                "previous_close_price": data.get("pc"),
                "Datetime": current_time
            }
            for symbol, data in self.symbol_prices_dict.items()
        ]
        df_stock_prices = pl.DataFrame(stock_rows)

        # Company information DataFrame
        company_rows = [
            {
                **data,
                "Datetime": current_time
            }
            for symbol, data_list in self.symbol_company_info_dict.items() for data in data_list
        ]
        df_company_info = pl.DataFrame(company_rows)

        return df_stock_prices, df_company_info

def usage():
    process = RetrieveRawDataProcess()
    df_stock_prices, df_company_info = process.retrieve_data_process_run()
    print("Stock Prices DataFrame:")
    print(df_stock_prices)
    print("\nCompany Information DataFrame:")
    print(df_company_info)