import sys

sys.path.append("/usr/local/modules")
from database_interaction import postgres_insert_df

import pandas as pd

def parse(file):
    column_names = ['event_instance_id', 'event_timestamp', 'channel', 'label', 'customer_id', 'ean_code',
                    'ean_code_grid_operator', 'collector', 'product', 'deleted', 'measurement_timestamp',
                    'measurement_value']
    df = pd.read_csv(
        file,
        index_col=None,
        sep=',',
        dtype=str,
        header=None, names=column_names)

    df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], unit='s')
    df['measurement_timestamp'] = pd.to_datetime(df['measurement_timestamp'], unit='s')

    df['deleted'] = df['deleted'].map({"false": False, "true": True})

    return df


def push(conn, df):
    postgres_insert_df(conn=conn, df=df, table="bird_historical_meter_raw")
    print("dataframe pushed to bird_historical_meter_raw")
    return True