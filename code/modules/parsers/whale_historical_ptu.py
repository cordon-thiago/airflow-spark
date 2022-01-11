import sys

sys.path.append("/usr/local/modules")
from database_interaction import postgres_insert_df

import logging
import pandas as pd
import numpy as np

def parse(file):
    df = pd.read_csv(
        file,
        sep='\t',
        index_col=False,
        names=['date', 'time', 'consumption'],
        skiprows=1,
        dtype={'date': 'str', 'time': 'str', 'consumption': 'float'},
    )

    ean_id = pd.read_csv(file, index_col=False, sep='\t', nrows=0).columns.tolist()[2]

    df['datetime'] = pd.to_datetime(
        df['date'] + " " + df['time'],
        format='%Y.%m.%d %H:%M'
    )
    # `infer` is absolutely fundamental, because the data messes with DST
    df['datetime'] = df['datetime'].dt.tz_localize('Europe/Amsterdam', ambiguous='infer').dt.tz_convert('UTC')

    df['connection_id'] = ean_id

    df = df[['connection_id', 'datetime', 'consumption']]

    # missing consumption figures are parsed as NaN, when should be None
    df = df.replace({np.nan: None})

    return df


def push(conn, df):
    postgres_insert_df(conn=conn, df=df, table="whale_historical_ptu")
    print("dataframe pushed to whale_historical_ptu")
    return True
