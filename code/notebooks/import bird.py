import requests
import zipfile
import os
import shutil

import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras as extras

sys.path.append("/usr/local/modules")
from database_interaction import postgres_insert_df


origin_folder = "/usr/local/remote_data/bird"

files_to_upload = [f"{origin_folder}/{x}" for x in os.listdir(origin_folder) if x.endswith(".csv")]


def parse_p4(file):
    column_names = ['event_instance_id','event_timestamp','channel','label','customer_id','ean_code','ean_code_grid_operator','collector','product','deleted','measurement_timestamp','measurement_value']
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
  
  
# for file in files_to_upload:
#     postgres_insert_df(
#         conn = conn,
#         df = parse_p4(file),
#         table = "bird_historical_meter_raw"
#         )
#     print(f"uploaded{file}")
# 
# 


  

######

conn = psycopg2.connect(
  host = "postgres",
  database = "test",
  user = "test",
  password = "postgres"
)

postgres_insert_df(
        conn = conn,
        df = df,
        table = "bird_historical_meter_raw"
        )



######

bird_meter_readings = df[["measurement_timestamp", "measurement_value", "channel", "product", "ean_code"]]
  
bird_meter_readings = bird_meter_readings.rename(
  columns = {
    "measurement_timestamp": "datetime",
    "measurement_value": "value"
  }
)
bird_meter_readings


ean_code = df[["ean_code", "customer_id", "label", "ean_code_grid_operator"]].iloc[[0]]
ean_code

###


        
        
        
