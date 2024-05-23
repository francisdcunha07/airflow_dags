from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import time

# Define default_args and DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_dag',
    default_args=default_args,
    description='A simple DAG for data transformation',
    schedule_interval= '5 * * * *' #timedelta(days=1),  # Adjust the interval as needed
)

# Create a sample DataFrame
data = {'Name': ['Alice', 'Bob', 'Charlie', 'David'],
        'Age': [25, 30, 35, 40]}
df_source = pd.DataFrame(data)

# Initialize an empty target DataFrame
df_target = pd.DataFrame(columns=['Name', 'Doubled_Age'])

# Define a function for your data transformation
def double_age():
    global df_target
    print("Performing data transformation...")
    df_transformed = df_source.copy()
    df_transformed['Doubled_Age'] = df_transformed['Age'] * 2
    df_target = df_transformed.copy()
    print("Data transformation completed.")

# Simulate time delays between tasks
def sleep(seconds):
    time.sleep(seconds)

# Define tasks in the DAG
task_get_data = PythonOperator(
    task_id='get_data',
    python_callable=sleep,
    op_args=[2],
    dag=dag,
)

task_transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=double_age,
    dag=dag,
)

task_store_data = PythonOperator(
    task_id='store_data',
    python_callable=sleep,
    op_args=[3],
    dag=dag,
)

# Define task dependencies
task_get_data >> task_transform_data >> task_store_data

if __name__ == "__main__":
    dag.cli()