import polars as pl

class DataProcessingClass:
    def __init__(self, stock_prices_df: pl.DataFrame, company_info_df: pl.DataFrame):
        self.stock_prices_df = stock_prices_df
        self.company_info_df = company_info_df
        self.silver_stock_prices_df = None
        self.silver_company_info_df = None
        self.gold_table_df = None

    @staticmethod
    def clean_data(df: pl.DataFrame) -> pl.DataFrame:
        """Remove rows with null values from the DataFrame."""
        return df.drop_nulls()

    @staticmethod
    def extract_datetime_components(df: pl.DataFrame, datetime_column: str = "Datetime") -> pl.DataFrame:
        """Extract Date, Day, Month, Year, Hour, Min, Sec from Datetime."""
        df = df.with_columns(
            pl.col(datetime_column).cast(pl.Utf8).str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S.%6f").alias("ParsedDatetime")
        ).with_columns([
            pl.col("ParsedDatetime").dt.date().alias("Date"),
            pl.col("ParsedDatetime").dt.day().alias("Day"),
            pl.col("ParsedDatetime").dt.month().alias("Month"),
            pl.col("ParsedDatetime").dt.year().alias("Year"),
            pl.col("ParsedDatetime").dt.hour().alias("Hour"),
            pl.col("ParsedDatetime").dt.minute().alias("Min"),
            pl.col("ParsedDatetime").dt.second().alias("Sec")
        ]).drop("ParsedDatetime")
        return df

    def prepare_silver_tables(self):
        """Clean and transform stock prices and company info DataFrames into Silver tables."""
        self.silver_stock_prices_df = self.clean_data(self.stock_prices_df)
        self.silver_stock_prices_df = self.extract_datetime_components(self.silver_stock_prices_df)

        self.silver_company_info_df = self.clean_data(self.company_info_df)
        self.silver_company_info_df = self.extract_datetime_components(self.silver_company_info_df)

    def create_gold_table(self):
        """Combine Silver tables to create a Gold table."""
        # Merge Silver tables on the stock symbol
        self.gold_table_df = self.silver_stock_prices_df.join(
            self.silver_company_info_df,
            left_on="stock_symbol",
            right_on="symbol",
            how="inner"
        )
        self.gold_table_df = self.gold_table_df.select([
            "stock_symbol",
            "companyName",
            "current_price",
            "percent_change",
            "mktCap",
            "sector",
            "industry",
            "country",
            "Date",
            "Day",
            "Month",
            "Year",
            "Hour",
            "Min",
            "Sec"
        ])

    def process_raw_data_and_retrieve_silver_tables(self):
        """Full data processing pipeline."""
        self.prepare_silver_tables()
        self.create_gold_table()
        return self.get_silver_stock_prices(), self.get_silver_company_info()

    def get_silver_stock_prices(self) -> pl.DataFrame:
        """Retrieve the Silver table for stock prices."""
        return self.silver_stock_prices_df

    def get_silver_company_info(self) -> pl.DataFrame:
        """Retrieve the Silver table for company information."""
        return self.silver_company_info_df

    def get_gold_table(self) -> pl.DataFrame:
        """Retrieve the Gold table."""
        return self.gold_table_df

def usage():
    df_stock_prices, df_company_info = None, None
    
    print("Bronze Stock Prices DataFrame:")
    print("\nBronze Stock Prices Columns: " + str(df_stock_prices.columns) + "\n")
    print("\nBronze Stock Prices Types: " + str(df_stock_prices.dtypes) + "\n")
    print(df_stock_prices)
    
    print("\nBronze Company Info DataFrame:")
    print("\nBronze Company Info Columns: " + str(df_company_info.columns) + "\n")
    print("\nBronze Company Info Types: " + str(df_company_info.dtypes) + "\n")
    print(df_company_info)
    
    processor = DataProcessingClass(df_stock_prices, df_company_info)
    silver_stock_prices, silver_company_info = processor.process_raw_data_and_retrieve_silver_tables()
    gold_table = processor.get_gold_table()
    
    print("\nSilver Stock Prices DataFrame:")
    print("\nSilver Stock Prices Columns: " + str(silver_stock_prices.columns) + "\n")
    print("\nSilver Stock Prices Types: " + str(silver_stock_prices.dtypes) + "\n")
    print(silver_stock_prices)
    
    print("\nSilver Company Info DataFrame:")
    print("\nSilver Company Info Columns: " + str(silver_company_info.columns) + "\n")
    print("\nSilver Company Info Types: " + str(silver_company_info.dtypes) + "\n")
    print(silver_company_info)
    
    print("\nGold Table DataFrame:")
    print("\nGold Columns: " + str(gold_table.columns) + "\n")
    print("\nnGold Types: " + str(gold_table.dtypes) + "\n")
    print(gold_table)