import sys

sys.path.append("/usr/local/modules")
from database_interaction import postgres_insert_df

import pandas as pd
import logging

def parse(file):
    df = pd.read_csv(
        file,
        sep=",",
        index_col=False,
        skiprows=_search_string_by_line(file, "# STN,YYYYMMDD")[0] - 1,
        skipinitialspace=True
    )

    station_code = df["# STN"][0]

    df['datetime'] = pd.to_datetime(df['YYYYMMDD'], format='%Y%m%d', utc=True)
    df['datetime'] = df['datetime'] + pd.to_timedelta(df['HH'] - 1, 'hours')

    df = df.rename(columns={"T": "temperature", "Q": "solar_radiation"})
    df['temperature'] = df['temperature'] / 10
    # pass from (J/h)/cm2 to W/m2
    df["solar_radiation"] = df["solar_radiation"] * ((100 * 100) / (60 * 60))
    df['station_id'] = station_code

    return df[['station_id', 'datetime', 'temperature', 'solar_radiation']]


def push(conn, df):
    postgres_insert_df(conn=conn, df=df, table="knmi_weather_hour")
    logging.info("dataframe pushed to knmi_weather_hour")
    return True

def _search_string_by_line(file_name, string_to_search):
    """Search for the given string in file and return line numbers with
    that string"""
    line_number = 0
    list_of_results = []
    # Open the file in read only mode
    with open(file_name, 'r') as read_obj:
        # Read all lines in the file one by one
        for line in read_obj:
            # For each line, check if line contains the string
            line_number += 1
            if string_to_search in line:
                # If yes, then add the line number & line as a tuple in the list
                list_of_results.append(line_number)
    return list_of_results
