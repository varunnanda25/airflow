from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
   'owner' : 'varun25'
}

def taskA():
    for i in range (1,6):
        for j in range(i):
            print('*', end='')
        print()    



def taskB():
    print("This is task b!")

def taskC():
    for i in range (6,0,-1):
        for j in range(i):
            print('*', end='')
        print()   

def taskD():
    print("This is task c!")


with DAG(
    dag_id='execute_python_operators',
    description='DAG with Python Operators',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=timedelta(days=1),
    tags=['python', 'operators']
) as dag:
    taskA=PythonOperator(
        task_id='taskA',
        python_callable=taskA
    )
    taskB=PythonOperator(
        task_id='taskB',
        python_callable=taskB
    )
    taskC=PythonOperator(
        task_id='taskC',
        python_callable=taskC
    )
    taskD=PythonOperator(
        task_id='taskD',
        python_callable=taskD
    )

taskA>>[taskB, taskC]
taskD<<[taskB, taskC]