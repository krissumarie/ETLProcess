import pandas as pd

CURRENCY_CODES = ["USD", "SEK", "GBP", "JPY"]

def extract_data_from_csv(daily_rates_file, historical_rates_file):
    """
    Extract and return the data in DataFrame format from the csv file, using pandas.
    :param historical_rates_file:
    :param daily_rates_file:
    :return: data
    """
    #Read the CSV file to a DataFrame format
    daily_data = pd.read_csv(daily_rates_file, skipinitialspace=True)
    historical_data = pd.read_csv(historical_rates_file, skipinitialspace=True)

    return daily_data, historical_data

def transform(daily_data, historical_data):
    """
    Select only necessary currency codes, calculate mean average.
    :param daily_data:
    :param historical_data:
    :return: mean_average, transformed_daily_rates
    """
    transformed_daily_rates = daily_data[CURRENCY_CODES].iloc[0] # To a series (1 dimensional)
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
    md_table = pd.DataFrame({"Currency code": CURRENCY_CODES,
                             "Rate": transformed_daily_rates.values,
                             "Mean historical rate": mean_average.values})
    return md_table

def run_etl_pipeline():

    daily, historical = extract_data_from_csv("csv_files/eurofxref.csv", "csv_files/eurofxref-hist.csv")
    mean_average, daily_transformed_data = transform(daily, historical)
    md_table = load_data(mean_average, daily_transformed_data)
    print(md_table)

if __name__ == "__main__":
    run_etl_pipeline()