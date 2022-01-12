import sys
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
import uuid
from pathlib import Path
import numpy as np
import os

import math
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error


sys.path.append("/usr/local/modules")
from database_interaction import sql_file_to_df, postgres_connect, postgres_insert_df
from whale_models import train_model, backcast_model

##
import atexit
conn = postgres_connect()

def close_connection():
    if 'conn' in locals(): conn.close()
    
atexit.register(close_connection)

###
df_ranges = sql_file_to_df(
      conn = conn,
      file = os.path.join("/usr/local/sql", "select", "connection_id_dttm_ranges.sql")
  )
  

###
    
for i in range(0,len(df_ranges)):
    ind_row = df_ranges.iloc[i]

    info_model = train_model(
        conn = conn,
        connection_id = ind_row.connection_id,
        date_from = ind_row.dttm_from,
        date_until = ind_row.dttm_08,
        method = "lm"
    )
    
    print(info_model)
    
    backcast_model(
      conn=conn, 
      model_id=info_model["id"],
      date_from = ind_row.dttm_08,
      date_until=ind_row.dttm_until
    )







