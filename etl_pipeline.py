import pandas as pd

CURRENCY_CODES = ["USD", "SEK", "GBP", "JPY"]
DAILY_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref.zip"
HISTORICAL_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"


def extract_csv_from_zip(url):
    """
    Try to extract csv file.
    :return: extracted csv file
    """
    return pd.read_csv(url, compression="zip", skipinitialspace=True)


def transform(daily_data, historical_data):
    """
    Select only necessary currency codes, calculate mean historical rates.
    :param daily_data:
    :param historical_data:
    :return: mean_rates, transformed_daily_rates
    """
    transformed_daily_rates = daily_data[CURRENCY_CODES].iloc[0] # To a Series (1 dimensional)
    transformed_historical_rates = historical_data[CURRENCY_CODES]

    mean_rates = transformed_historical_rates.mean(skipna=True)
    return mean_rates, transformed_daily_rates


def load_data(mean_rates, transformed_daily_rates):
    """
    Create a Markdown table and save it into a file.
    :param mean_rates:
    :param transformed_daily_rates:
    :return: md_table
    """
    md_table = pd.DataFrame(
        {
            "Currency code": transformed_daily_rates.index,
            "Rate": transformed_daily_rates.values,
            "Mean historical rate": mean_rates.values
        }
    )

    md_table.to_markdown("exchange_rates.md", index=False)
    return md_table


def run_etl_pipeline():
    """
    Run the ETL process. First read csv from the url using pandas, then transform the csv files and
    calculate the mean historical rates. Then load the data to a markdown file.
    """
    try:
        daily_df = extract_csv_from_zip(DAILY_URL)
        historical_df = extract_csv_from_zip(HISTORICAL_URL)

        mean_rates, daily_transformed_data = transform(daily_df, historical_df)
        load_data(mean_rates, daily_transformed_data)
        print("ETL pipeline succeeded")
    except Exception as e:
        print(f"ETL pipeline failed: {e}")


if __name__ == "__main__":
    run_etl_pipeline()