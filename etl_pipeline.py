import pandas as pd

def extract_data_from_csv(daily_rates, historical_rates):
    """
    Extract and return the data in DataFrame format from the csv file, using pandas.
    :param file:
    :return: data
    """
    daily_data = pd.read_csv(daily_rates, skipinitialspace=True)
    historical_data = pd.read_csv(historical_rates, skipinitialspace=True)

    return daily_data, historical_data

def transform(daily_rates, historical_rates):

    transformed_daily_rates = daily_rates[["USD", "SEK", "GBP", "JPY"]]
    transformed_historical_rates = historical_rates[["USD", "SEK", "GBP", "JPY"]]

    mean_average = transformed_historical_rates.mean()
    return mean_average


def run_etl_pipeline():
    daily, historical = extract_data_from_csv("csv_files/eurofxref.csv", "csv_files/eurofxref-hist.csv")
    transformed_data = transform(daily, historical)
    print(transformed_data)

run_etl_pipeline()