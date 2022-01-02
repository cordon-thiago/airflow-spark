from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import psycopg2

import requests
import zipfile
import os

import pandas as pd
import datetime
import sys

sys.path.append("/usr/local/modules")
from parsers.knmi_weather_hour import parse, push


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

def download_data():
    # just files from de_bilt station (for showcase)
    files_to_download = ["uurgeg_260_2011-2020.zip", "uurgeg_260_2021-2030.zip"]

    cache_folder = "/tmp/de_bilt_cache"
    
    # create cache folder
    if os.path.exists(cache_folder) is False:
        os.mkdir(cache_folder)
    
    for file in files_to_download:
      download_uurgegevens_txt(file, cache_folder)
      print(f"downloaded {file} in {cache_folder}")
      
    return True
  
def parse_and_upload():
  
  cache_folder = "/tmp/de_bilt_cache"
  
  files_to_upload = [f"{cache_folder}/{x}" for x in os.listdir(cache_folder) if x.endswith(".txt")]
  
  conn = psycopg2.connect(
        host = "postgres",
        database = "test",
        user = "test",
        password = "postgres"
      )
  
  for file in files_to_upload:
      push(conn=conn, df=parse(file))
      
  conn.close()
      
  return True
  
# DAG
dag = DAG(
    dag_id="populate_knmi_weather_hour",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
    tags=['setup']
)

start_populate = DummyOperator(
        task_id='start_populate',
        dag=dag)

download_data = PythonOperator(
    task_id="download_data",
    python_callable=download_data,
    dag=dag
)

parse_and_upload_data = PythonOperator(
    task_id="parse_and_upload_data",
    python_callable=parse_and_upload,
    dag=dag
)

end_populate = DummyOperator(
    task_id='end_populate',
    dag=dag)
      
start_populate >> download_data >> parse_and_upload_data >> end_populate
