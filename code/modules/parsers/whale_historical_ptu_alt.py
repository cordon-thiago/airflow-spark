import sys

sys.path.append("/usr/local/modules")
from database_interaction import postgres_insert_df

import logging
import pandas as pd

def parse(file):
    df = pd.read_csv(
        file,
        index_col=False,
        parse_dates = ['datetime'],
    )

    return df


def push(conn, df):
    postgres_insert_df(conn=conn, df=df, table="whale_historical_ptu")
    logging.info("dataframe pushed to whale_historical_ptu")
    return True
