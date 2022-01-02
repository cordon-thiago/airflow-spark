from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator

import os
import datetime
from pathlib import Path

sql_folder = "/usr/local/sql/create_tables"

dag = DAG(
    dag_id="create_timescale_tables",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
    template_searchpath=sql_folder,
    tags=['setup', 'foremost']
)
    
start_queries = DummyOperator(
        task_id='start_queries',
        dag=dag)
        
end_queries = DummyOperator(
        task_id='end_queries',
        dag=dag)
        
queries_to_execute = [x for x in os.listdir(sql_folder) if x.endswith(".sql")]

for sql_file in queries_to_execute:
    dynamic_query = PostgresOperator(
        dag=dag,
        task_id=Path(sql_file).stem,
        sql=sql_file,
        postgres_conn_id="postgres_test"
    )
    
    start_queries.set_downstream(dynamic_query)
    dynamic_query.set_downstream(end_queries)
    
