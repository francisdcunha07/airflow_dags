from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Define the DAG
dag = DAG(
    'simple_dag',
    description='A simple DAG',
    start_date=datetime(2023, 6, 1),
    schedule_interval=None,
)

# Define tasks
task1 = EmptyOperator(task_id='task1', dag=dag)
task2 = EmptyOperator(task_id='task2', dag=dag)

# Define task dependencies
task1 >> task2
