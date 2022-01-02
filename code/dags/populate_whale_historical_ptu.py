from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import psycopg2

import pandas as pd
import datetime
import sys
import os

sys.path.append("/usr/local/modules")
from parsers.whale_historical_ptu import parse, push

  
def parse_and_upload():
  
    origin_folder = "/usr/local/remote_data/whale"

    files_to_upload = [f"{origin_folder}/{x}" for x in os.listdir(origin_folder) if x.endswith(".csv")]
    
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
    dag_id="populate_whale_historical_ptu",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
    tags=['setup']
)

start_populate = DummyOperator(
        task_id='start_populate',
        dag=dag)

parse_and_upload_data = PythonOperator(
    task_id="parse_and_upload_data",
    python_callable=parse_and_upload,
    dag=dag
)

end_populate = DummyOperator(
    task_id='end_populate',
    dag=dag)
      
start_populate >> parse_and_upload_data >> end_populate
