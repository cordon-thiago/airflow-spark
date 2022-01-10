from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

import logging

import datetime
import sys

sys.path.append("/usr/local/modules")
from database_interaction import postgres_connect
from whale_models import train_model

import atexit

def close_connection():
    if 'conn' in locals(): conn.close()

def do_train_one_model(**context):
    
    conn = postgres_connect()

    def close_connection():
        if 'conn' in locals(): conn.close()
    atexit.register(close_connection)
  
    info_model = train_model(
        conn = conn,
        connection_id = context["dag_run"].conf["connection_id"],
        date_from = context["dag_run"].conf["date_from"],
        date_until = context["dag_run"].conf["date_until"],
        method = context["dag_run"].conf["method"]
    )
    logging.info(info_model) 

# DAG
dag = DAG(
    dag_id="train_one_model",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
    tags=['run']
)

start_train = DummyOperator(
        task_id='start_train',
        dag=dag)

train_one_model_operator = PythonOperator(
    task_id="do_train_one_model",
    python_callable=do_train_one_model,
    provide_context=True,
    dag=dag
)

end_train = DummyOperator(
    task_id='end_train',
    dag=dag)
      
start_train >> train_one_model_operator >> end_train
