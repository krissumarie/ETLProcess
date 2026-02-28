import pandas as pd

CURRENCY_CODES = ["USD", "SEK", "GBP", "JPY"]

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

    transformed_daily_rates = daily_rates[CURRENCY_CODES]
    transformed_historical_rates = historical_rates[CURRENCY_CODES]

    mean_average = transformed_historical_rates.mean()
    return mean_average, transformed_daily_rates

def load_data(mean_average, transformed_daily_rates):
    md_table = pd.DataFrame({"CURRENCY CODES": CURRENCY_CODES,
                   "MEAN_AVERAGE": mean_average})
    return md_table

def run_etl_pipeline():
    daily, historical = extract_data_from_csv("csv_files/eurofxref.csv", "csv_files/eurofxref-hist.csv")
    mean_average, daily_transformed_data = transform(daily, historical)
    md_table = load_data(mean_average, daily_transformed_data)
    print(md_table)

run_etl_pipeline()