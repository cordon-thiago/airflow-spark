import sys

sys.path.append("/usr/local/modules")
from database_interaction import postgres_insert_df

import logging
import pandas as pd
import numpy as np
from workalendar.europe import Netherlands

def generate():
    initial_local_time = pd.date_range('2010-01-01', '2030-01-01', freq='15min', tz='Europe/Amsterdam')
    initial_utc_time = initial_local_time.tz_convert("UTC")

    dttm_df = pd.DataFrame({
        "datetime_amsterdam": initial_local_time,
        "datetime": initial_utc_time
    })

    dttm_df["is_holiday"] = dttm_df["datetime_amsterdam"].apply(lambda y: Netherlands().is_holiday(y))

    # Monday=0, Sunday=6
    dttm_df["week_day"] = dttm_df["datetime_amsterdam"].dt.dayofweek

    dttm_df["seconds_from_midnight"] = (
            (dttm_df["datetime_amsterdam"] - dttm_df["datetime_amsterdam"].dt.normalize()) / pd.Timedelta(
        '1 second')).astype(int)
    dttm_df['sfm_sin'] = _sin_encode(dttm_df["seconds_from_midnight"], (60 * 60 * 24))
    dttm_df['sfm_cos'] = _cos_encode(dttm_df["seconds_from_midnight"], (60 * 60 * 24))

    dttm_df["day_of_year"] = dttm_df["datetime_amsterdam"].dt.dayofyear
    dttm_df['doy_sin'] = _sin_encode(dttm_df["day_of_year"] - 1, 365 - 1)
    dttm_df['doy_cos'] = _cos_encode(dttm_df["day_of_year"] - 1, 365 - 1)

    return dttm_df[["datetime", "week_day", "is_holiday", "sfm_sin", "sfm_cos", "doy_sin", "doy_cos"]]


def push(conn, df):
    postgres_insert_df(conn=conn, df=df, table="datetime_amsterdam_properties")
    logging.info("dataframe pushed to datetime_amsterdam_properties")
    return True

def _sin_encode(x, max):
  return np.sin(2 * np.pi * (x/max))

def _cos_encode(x, max):
  return np.cos(2 * np.pi * (x/max))
