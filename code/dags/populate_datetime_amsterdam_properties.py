from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import psycopg2

import pandas as pd
import datetime
import sys
import os

sys.path.append("/usr/local/modules")
from generators.datetime_amsterdam_properties import generate, push
  
def generate_and_upload():
  
    conn = psycopg2.connect(
          host = "postgres",
          database = "test",
          user = "test",
          password = "postgres"
        )
        
    push(conn=conn, df=generate())
        
    conn.close()
        
    return True
  
# DAG
dag = DAG(
    dag_id="populate_datetime_amsterdam_properties",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
    tags=['setup']
)

start_populate = DummyOperator(
        task_id='start_populate',
        dag=dag)

generate_and_upload = PythonOperator(
    task_id="generate_and_upload",
    python_callable=generate_and_upload,
    dag=dag
)

end_populate = DummyOperator(
    task_id='end_populate',
    dag=dag)
      
start_populate >> generate_and_upload >> end_populate
