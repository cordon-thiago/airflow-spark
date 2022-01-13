from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

import logging

import datetime
import sys
import os

sys.path.append("/usr/local/modules")
from database_interaction import postgres_connect, sql_file_to_df
from whale_models import train_model, backcast_model

import atexit

def do_all_whale_model_backcast():
    """
    Loop over all the ids; do a initial training with 80% of the data, and 
    backcast (evaluate) with the remaining 20%
    """
    
    conn = postgres_connect()

    def close_connection():
        if 'conn' in locals(): conn.close()
    atexit.register(close_connection)

    df_ranges = sql_file_to_df(
      conn = conn,
      file = os.path.join("/usr/local/sql", "select", "connection_id_dttm_ranges.sql")
    )
  
    for i in range(0,len(df_ranges)):
        ind_row = df_ranges.iloc[i]
    
        info_model = train_model(
            conn = conn,
            connection_id = ind_row.connection_id,
            date_from = ind_row.dttm_from,
            date_until = ind_row.dttm_08,
            method = "lr"
        )
        
        logging.info(info_model)
        
        backcast_model(
          conn=conn, 
          model_id=info_model["id"],
          date_from = ind_row.dttm_08,
          date_until=ind_row.dttm_until
        )
    
    return True
        
        

# DAG
dag = DAG(
    dag_id="run_all_whale_model_backcast",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval=None,
    catchup=False,
    tags=['setup', 'run']
)

start_process = DummyOperator(
        task_id='start_process',
        dag=dag)

run_all_whale_model_backcast_operator = PythonOperator(
    task_id="do_all_whale_model_backcast",
    python_callable=do_all_whale_model_backcast,
    provide_context=True,
    dag=dag
)

end_process = DummyOperator(
    task_id='end_process',
    dag=dag)
      
start_process >> run_all_whale_model_backcast_operator >> end_process
