# from Data_Engineering_Pipeline.tasks.Load_Data_Into_PostgreSQL_Processing import LoadDataProcess
# from Data_Engineering_Pipeline.tasks.Retrieve_Raw_Data_Processing import RetrieveRawDataProcess
# from Data_Engineering_Pipeline.tasks.Preprocessing_Data import DataProcessingClass
from tasks.Load_Data_Into_PostgreSQL_Processing import LoadDataProcess
from tasks.Retrieve_Raw_Data_Processing import RetrieveRawDataProcess
from tasks.Preprocessing_Data import DataProcessingClass
from prefect import task, flow
from datetime import timedelta
import os

@task
def retrieve_raw_data():
    process = RetrieveRawDataProcess()
    df_stock_prices, df_company_info = process.retrieve_data_process_run()
    return df_stock_prices, df_company_info

@task
def process_data(df_stock_prices_, df_company_info_):
    processor = DataProcessingClass(df_stock_prices_, df_company_info_)
    _silver_stock_prices, _silver_company_info = processor.process_raw_data_and_retrieve_silver_tables()
    _gold_table = processor.get_gold_table()
    return _silver_stock_prices, _silver_company_info, _gold_table

@task
def load_data_to_postgres(df_stock_prices__, df_company_info__, silver_stock_prices_, silver_company_info_, gold_table_):
    dataframes = {
        "stock_prices_fact_table": df_stock_prices__,
        "company_information_dim_table": df_company_info__,
        "silver_stock_prices_fact": silver_stock_prices_,
        "silver_company_info_dim": silver_company_info_,
        "gold_table": gold_table_,
        "gold_company_information_dim_table": silver_company_info_,
    }
    loader = LoadDataProcess()
    loader.load_all_data(dataframes)
    print("Data loaded into PostgreSQL.")

@flow(log_prints=True)
def data_retrieve_pipeline_flow():
    df_stock_prices, df_company_info = retrieve_raw_data()
    silver_stock_prices, silver_company_info, gold_table = process_data(df_stock_prices, df_company_info)
    load_data_to_postgres(df_stock_prices, df_company_info, silver_stock_prices, silver_company_info, gold_table)

if __name__ == "__main__":
    data_retrieve_pipeline_flow.serve(
        name="data-retrieve-pipeline-flow",
        interval=timedelta(minutes=2)
    )
