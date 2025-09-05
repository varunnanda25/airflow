from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
   'owner' : 'varun25'
}

def hello(name):
    print(f'Hello {name}!'.format(name=name))

def hello_with_city(name, city):
    print(f'Hello {name}! from {city}.'.format(name=name, city=city))

def increment_by_2(counter):
    print("count{counter}".format(counter=counter))
    return counter+2

def multiply_by_2(counter):
    print("count{counter}".format(counter=counter))
    return counter*2


with DAG(
    dag_id='execute_python_operators',
    description='DAG with Python Operators',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=timedelta(days=1),
    tags=['python', 'operators']
) as dag:
    taskA=PythonOperator(
        task_id='hello',
        python_callable=hello,
        op_kwargs={'name':'Varun'} # specifying argument for the function
    )
    taskB=PythonOperator(
        task_id='hello_with_city',
        python_callable=hello_with_city,
        op_kwargs={'name':'Varun', 'city':'Pathankot'} # specifying arguments for the function
    )

taskA>>taskB