from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

import logging

import datetime
import sys

sys.path.append("/usr/local/modules")
from database_interaction import postgres_connect
from whale_models import backcast_model

import atexit

def do_backcast_one_model(**context):
    
    conn = postgres_connect()

    def close_connection():
        if 'conn' in locals(): conn.close()
    atexit.register(close_connection)
  
    info_model = backcast_model(
        conn = conn,
        model_id = context["dag_run"].conf["model_id"],
        date_from = context["dag_run"].conf["date_from"],
        date_until = context["dag_run"].conf["date_until"]
    )
    logging.info(info_model) 

# DAG
dag = DAG(
    dag_id="backcast_one_model",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
    tags=['run']
)

start_backcast = DummyOperator(
        task_id='start_backcast',
        dag=dag)

backcast_one_model_operator = PythonOperator(
    task_id="do_backcast_one_model",
    python_callable=do_backcast_one_model,
    provide_context=True,
    dag=dag
)

end_backcast = DummyOperator(
    task_id='end_backcast',
    dag=dag)
      
start_backcast >> backcast_one_model_operator >> end_backcast
