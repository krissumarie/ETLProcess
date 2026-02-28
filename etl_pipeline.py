import pandas as pd

def extract_data_from_csv(file):
    """
    Extract and return the data in DataFrame format from the csv file, using pandas.
    :param file:
    :return: data
    """
    data = pd.read_csv(file)
    return data


#Testing
data = extract_data_from_csv("csv_files/eurofxref.csv")
print(data)