import requests
import zipfile
import os
import shutil

import logging
import pandas as pd
import psycopg2
import psycopg2.extras as extras

###
def search_string_by_line(file_name, string_to_search):
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
  
def download_uurgegevens_txt (file_name, destination_folder):
    """
    Download uurgegevens data from KNMI
    """
    url = f"https://cdn.knmi.nl/knmi/map/page/klimatologie/gegevens/uurgegevens/{file_name}"
    r = requests.get(url, allow_redirects=True)
    
    open(f"{destination_folder}/{file_name}", 'wb').write(r.content)
    
    with zipfile.ZipFile(f"{destination_folder}/{file_name}", 'r') as zip_ref:
        zip_ref.extractall(f"{destination_folder}")
        
    return True
  
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
        logging.error("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    logging.info("execute_values() done")
    cursor.close()

###

files_to_download = ["uurgeg_260_2011-2020.zip", "uurgeg_260_2021-2030.zip"]

cache_folder = "/tmp/de_bilt_cache"

# create cache folder
if os.path.exists(cache_folder) is False:
    os.mkdir(cache_folder)



for file in files_to_download:
  download_uurgegevens_txt(file, cache_folder)
  
  


# remove cache folder
#if os.path.exists(cache_folder) is True:
#    shutil.rmtree(cache_folder)


####

files_to_upload = [f"{cache_folder}/{x}" for x in os.listdir(cache_folder) if x.endswith(".txt")]

def parse_knmi_txt (file):
    df = pd.read_csv(
        file,
        sep = ",",
        index_col=False,
        skiprows=search_string_by_line(file, "# STN,YYYYMMDD")[0] -1,
        skipinitialspace=True
        )
        
    station_code = df["# STN"][0]
    
    df['datetime'] = pd.to_datetime(df['YYYYMMDD'], format='%Y%m%d', utc=False)
    df['datetime'] = df['datetime'].dt.tz_localize('CET')
    df['datetime'] = df['datetime'] + pd.to_timedelta(df['HH']-1, 'hours')
    
    df = df.rename(columns={"T": "temperature", "Q": "solar_radiation"})
    df['temperature'] = df['temperature']/10
    # pass from (J/h)/cm2 to W/m2
    df["solar_radiation"] = df["solar_radiation"] * ((100*100)/(60 * 60))
    df['station_id'] = station_code
    
    return df[['station_id', 'datetime', 'temperature', 'solar_radiation']]
  

parse_knmi_txt(files_to_upload[0])


df_to_upload = pd.concat(list(map(parse_knmi_txt, files_to_upload)))

df_to_upload = df_to_upload.drop_duplicates(subset=['datetime'])

###

conn = psycopg2.connect(
  host = "postgres",
  database = "test",
  user = "test",
  password = "postgres"
)

postgres_insert_df(conn = conn, df = df_to_upload, table = "knmi_weather_hour")



