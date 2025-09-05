from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
   'owner' : 'loonycorn'
}

def print_function():
    print("This is a python operator!")

with DAG(
    dag_id='execute_python_operators',
    description='DAG with Python Operators',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=timedelta(days=1),
    tags=['python', 'operators']
) as dag:
    task=PythonOperator(
        task_id='python_task',
        python_callable=print_function
    )

task