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

def remove_null_values(**kwargs):
    ti=kwargs['ti']
    json_data=ti.xcom_pull(task_ids='read_csv')
    df=pd.read_json(json_data)
    df=df.dropna()
    print(df)
    return df.to_json()

def groupby_smoker(ti):
    json_data=ti.xcom_pull(task_ids='remove_null_values')
    df=pd.read_json(json_data)
    smoker_df=df.groupby('smoker').agg({
        'charges':'mean',
        'age':'mean',
        'bmi':'mean'
        }).reset_index()# calculating the mean chanrges, age and bmi , grouperd by smoker
    smoker_df.to_csv('/home/varun/airflow/output/smoker_analysis.csv', index=False)

def groupby_region(ti):
    json_data=ti.xcom_pull(task_ids='remove_null_values')
    df=pd.read_json(json_data)
    region_df=df.groupby('region').agg({
        'charges':'mean',
        'age':'mean',
        'bmi':'mean'
        }).reset_index() # calculating the mean chanrges, age and bmi , grouperd by region
    region_df.to_csv('/home/varun/airflow/output/region_analysis.csv', index=False)


with DAG(
    dag_id = 'python_pipeline',
    description = 'DAG with multiple tasks and dependencies',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = timedelta(days=1),
    tags= ['python', 'ransform','pipeline'],
    template_searchpath=['/home/varun/airflow/dags/bash scripts/']
    
) as dag:
    read_csv_file= PythonOperator(
        task_id='read_csv',
        python_callable=read_csv
    )
    remove_null=PythonOperator(
        task_id='remove_null_values',
        python_callable=remove_null_values,
    )
    groupby_smoker_task=PythonOperator(
        task_id='groupby_smoker',
        python_callable=groupby_smoker
    )
    groupby_region_task=PythonOperator(
        task_id='groupby_region',
        python_callable=groupby_region
    )
read_csv_file>>remove_null>>[groupby_smoker_task, groupby_region_task]