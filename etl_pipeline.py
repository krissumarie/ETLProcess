import io
import zipfile

import pandas as pd
import requests

CURRENCY_CODES = ["USD", "SEK", "GBP", "JPY"]
DAILY_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref.zip"
HISTORICAL_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"
DATA_DIR = "ecb_data"

def extract_csv_from_zip(url):
    """
    Try to extract csv file.
    :return: extracted csv file
    """
    return pd.read_csv(url, compression="zip", skipinitialspace=True)

def extract_data_from_csv(daily_rates_file, historical_rates_file):
    """
    Extract and return the data in DataFrame format from the csv file, using pandas.
    :param historical_rates_file:
    :param daily_rates_file:
    :return: data
    """
    #Read the CSV file to a DataFrame format

    daily_data = pd.read_csv(daily_rates_file)
    historical_data = pd.read_csv(historical_rates_file)

    return daily_data, historical_data

def transform(daily_data, historical_data):
    """
    Select only necessary currency codes, calculate mean historical rates.
    :param daily_data:
    :param historical_data:
    :return: mean_average, transformed_daily_rates
    """
    transformed_daily_rates = daily_data[CURRENCY_CODES].iloc[0] # To a Series (1 dimensional)
    transformed_historical_rates = historical_data[CURRENCY_CODES]

    mean_average = transformed_historical_rates.mean(skipna=True)
    return mean_average, transformed_daily_rates

def load_data(mean_average, transformed_daily_rates):
    """
    Create a Markdown table and save it into a file.
    :param mean_average:
    :param transformed_daily_rates:
    :return: md_table
    """
    md_table = pd.DataFrame({"Currency code": transformed_daily_rates.index,
                             "Rate": transformed_daily_rates.values,
                             "Mean historical rate": mean_average.values})

    md_table.to_markdown("exchange_rates.md", index=False)
    return md_table

def run_etl_pipeline():
    """
    Run the ETL process
    """
    daily_df = extract_csv_from_zip(DAILY_URL)
    historical_df = extract_csv_from_zip(HISTORICAL_URL)

    mean_average, daily_transformed_data = transform(daily_df, historical_df)
    load_data(mean_average, daily_transformed_data)

if __name__ == "__main__":
    run_etl_pipeline()