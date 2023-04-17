from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from crypto_scraper_code import main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 11),
    'email': ['sourabhsinghrana.1251@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'crypto_dag_outer',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval=timedelta(minutes=1),
    catchup=False
)

scrape = PythonOperator(
    task_id='scrape_data_from_crypto.com',
    python_callable=main,
    dag=dag, 
)


# Creating first task
start = DummyOperator(task_id = 'start', dag = dag)


# Creating second task
end = DummyOperator(task_id = 'end', dag = dag)


# Setting up dependencies 
start >> scrape >> end 
# We can also write it as start.set_downstream(end) 
