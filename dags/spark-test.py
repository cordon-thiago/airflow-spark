from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
        "spark-test", 
        default_args=default_args, 
        schedule_interval=timedelta(1),
        #template_searchpath="./sql/etl/"
    )

start = DummyOperator(task_id="start", dag=dag)

spark_job = SparkSubmitOperator(
    task_id="spark_job",
    application="/usr/local/airflow/spark-scripts/hello-world.py",
    conn_id="spark_default",
    verbose=1,
    dag=dag)

start >> spark_job