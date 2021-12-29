import requests
import zipfile
import os
import shutil

import pandas as pd
import psycopg2
import psycopg2.extras as extras


def postgres_insert_df(conn, df, table):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()

###


origin_folder = "/usr/local/remote_data/whale"

files_to_upload = [f"{origin_folder}/{x}" for x in os.listdir(origin_folder) if x.endswith(".csv")]

def parse_ean_csv(file):
    df = pd.read_csv(
        file,
        sep='\t',
        index_col=False,
        names = ['date', 'time', 'consumption'],
        skiprows=1,
        dtype = {'date': 'str', 'time':'str', 'consumption':'float'},
        )
    
    ean_id = pd.read_csv(file, index_col=False, sep='\t', nrows=0).columns.tolist()[2]
    
    df['datetime'] = pd.to_datetime(
        df['date'] + " " + df['time'],
        format='%Y.%m.%d %H:%M'
        )
    # `infer` is absolutely fundamental, because the data messes with DST
    df['datetime'] = df['datetime'].dt.tz_localize('Europe/Amsterdam', ambiguous = 'infer').dt.tz_convert('UTC')
    
    df['connection_id'] = ean_id
    
    df = df[['connection_id', 'datetime', 'consumption']]
  
    # missing consumption figures are parsed as NaN, when should be None
    df = df.replace({np.nan: None})
    
    return df
  

### upload

conn = psycopg2.connect(
  host = "postgres",
  database = "test",
  user = "test",
  password = "postgres"
)

for file in files_to_upload:
    postgres_insert_df(
        conn = conn,
        df = parse_ean_csv(file),
        table = "whale_historical_ptu"
        )
    print(f"uploaded{file}")


###





