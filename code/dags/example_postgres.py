import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

with DAG(
    dag_id="postgres_operator_dag",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
) as dag:
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
          """,
          postgres_conn_id="postgres_test"
    )
    populate_pet_table = PostgresOperator(
        task_id="populate_pet_table",
        sql="""
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');
            """,
            postgres_conn_id="postgres_test"
    )
    
    get_all_pets = PostgresOperator(
      task_id="get_all_pets",
      sql="SELECT * FROM pet;",
      postgres_conn_id="postgres_test"
      )
    
    get_birth_date = PostgresOperator(
        task_id="get_birth_date",
        sql="""
            SELECT * FROM pet
            WHERE birth_date
            BETWEEN SYMMETRIC DATE '{{ params.begin_date }}' AND DATE '{{ params.end_date }}';
            """,
        params={'begin_date': '2020-01-01', 'end_date': '2020-12-31'},
        postgres_conn_id="postgres_test"
    )

    create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date
