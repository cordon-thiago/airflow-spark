from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator

import datetime

dag = DAG(
    dag_id="wipe_all_whale_model_backcast",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval=None,
    catchup=False,
    tags=['reboot']
)
    
start_wiping = DummyOperator(
        task_id='start_wiping',
        dag=dag)
        
        
end_wiping = DummyOperator(
        task_id='end_wiping',
        dag=dag)
        
truncate_whale_backcast = PostgresOperator(
        dag=dag,
        task_id="truncate_whale_backcast",
        sql="TRUNCATE TABLE whale_backcast",
        postgres_conn_id="postgres_test"
    )
    
truncate_whale_models = PostgresOperator(
        dag=dag,
        task_id="truncate_whale_models",
        sql="TRUNCATE TABLE whale_models",
        postgres_conn_id="postgres_test"
    )

rm_folder_whale_backcast = BashOperator(
    task_id='rm_folder_whale_backcast',
    bash_command='rm /usr/local/remote_data/whale_backcast/*',
)

rm_folder_whale_models = BashOperator(
    task_id='rm_folder_whale_models',
    bash_command='rm /usr/local/remote_data/whale_models/*',
)  

start_wiping >> truncate_whale_backcast >> end_wiping
start_wiping >> truncate_whale_models >> end_wiping
start_wiping >> rm_folder_whale_backcast >> end_wiping
start_wiping >> rm_folder_whale_models >> end_wiping
