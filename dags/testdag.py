from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_start():
    print("DAG start")

def print_processing():
    print("Processing data")

def print_insertion_complete():
    print("Insertion complete")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('test_dag_total',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=print_start
    )

    processing_task = PythonOperator(
        task_id='processing_task',
        python_callable=print_processing
    )

    insertion_complete_task = PythonOperator(
        task_id='insertion_complete_task',
        python_callable=print_insertion_complete
    )

    start_task >> processing_task >> insertion_complete_task