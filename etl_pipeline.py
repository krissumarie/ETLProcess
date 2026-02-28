import pandas as pd

def extract_data_from_csv(file):
    """
    Extract and return the data in DataFrame format from the csv file, using pandas.
    :param file:
    :return: data
    """
    data = pd.read_csv(file)
    return data

def transform(daily_rates, historical_rates):

    transformed_daily_rates = daily_rates[[" USD", " SEK", " GBP", " JPY"]]
    return transformed_daily_rates


def run_etl_pipeline():
    data = extract_data_from_csv("csv_files/eurofxref.csv")
    transformed_data = transform(data)
    print(data)
    print(transformed_data)

run_etl_pipeline()