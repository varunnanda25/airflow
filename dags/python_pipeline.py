import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
   'owner' : 'varun25'
}

def read_csv():
    df=pd.read_csv('/home/varun/airflow/datasets/insurance.csv')
    print(df.head())
    return df.to_json()

with DAG(
    dag_id = 'python_pipeline',
    description = 'DAG with multiple tasks and dependencies',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = timedelta(days=1),
    tags= ['python', 'pandas'],
    template_searchpath=['/home/varun/airflow/dags/bash scripts/']
    
) as dag:
    task= PythonOperator(
        task_id='read_csv',
        python_callable=read_csv
    )
task