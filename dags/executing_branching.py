from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label

default_args = {
    'owner' : 'varun25',
}

dataset_path='/home/varun/airflow/datasets/insurance.csv'
output_path='/home/varun/airflow/output/{0}.csv'



def read_csv_file(ti):
    df = pd.read_csv(dataset_path)
    print(df.head())
    ti.xcom_push(key='my_csv', value=df.to_json())  #pushing dataframe as json object to xcom

def remove_null_values(ti):
    json_data=ti.xcom_pull(key='my_csv')
    df=pd.read_json(json_data)
    df=df.dropna()
    print(df)
    ti.xcom_push(key='my_clean_csv', value=df.to_json())

def determine_branch():
    transform_action=Variable.get('transform_action', default_var=None)
    if transform_action.startswith('filter'):
        return 'filtering.{0}'.format(transform_action)
    elif transform_action=='groupby_region_smoker':
        return 'grouping.{0}'.format(transform_action)
    
def filter_by_southwest(ti):
    json_data=ti.xcom_pull(key='my_clean_csv')
    df=pd.read_json(json_data)
    filtered_df=df[df['region']=='southwest']
    filtered_df.to_csv(output_path.format('southwest'), index=False)

def filter_by_southeast(ti):
    json_data=ti.xcom_pull(key='my_clean_csv')
    df=pd.read_json(json_data)
    filtered_df=df[df['region']=='southeast']
    filtered_df.to_csv(output_path.format('southeast'), index=False)

def filter_by_northwest(ti):
    json_data=ti.xcom_pull(key='my_clean_csv')
    df=pd.read_json(json_data)
    filtered_df=df[df['region']=='northwest']
    filtered_df.to_csv(output_path.format('northwest'), index=False)

def filter_by_northeast(ti):
    json_data=ti.xcom_pull(key='my_clean_csv')
    df=pd.read_json(json_data)
    filtered_df=df[df['region']=='northeast']
    filtered_df.to_csv(output_path.format('northeast'), index=False)


def groupby_region_smoker(ti):
    json_data=ti.xcom_pull(key='my_clean_csv')
    df=pd.read_json(json_data)
    region_smoker_df=df.groupby(['region']).agg({
        'charges':'mean',
        'age':'mean',
        'bmi':'mean'
        }).reset_index() # calculating the mean chanrges, age and bmi , grouperd by region and smoker
    region_smoker_df.to_csv(output_path.format('grouped_by_region'), index=False)

    smoker_df=df.groupby(['smoker']).agg({
        'charges':'mean',
        'age':'mean',
        'bmi':'mean'
        }).reset_index() # calculating the mean chanrges, age and bmi , grouperd by region and smoker
    smoker_df.to_csv(output_path.format('grouped_by_smoker'), index=False)



with DAG(
    dag_id = 'executing_branching',
    description = 'Running branching pipelines',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['python', 'branching', 'pipeline', 'transform']
) as dag:
    
    with TaskGroup('reading_and_processing') as reading_and_processing:

        read_csv_file_task = PythonOperator(
            task_id = 'read_csv_file',
            python_callable = read_csv_file             
        )

        remove_null_values_task = PythonOperator(
            task_id = 'remove_null_values',
            python_callable = remove_null_values              
        )
        read_csv_file_task >> remove_null_values_task

    determine_branch_task = BranchPythonOperator(
        task_id = 'determine_branch',
        python_callable = determine_branch
    )

    with TaskGroup('filtering') as filtering:
        filter_by_northeast_task = PythonOperator(
            task_id = 'filter_by_northeast',
            python_callable = filter_by_northeast              
        )
        filter_by_northwest_task = PythonOperator(
            task_id = 'filter_by_northwest',
            python_callable = filter_by_northwest              
        )
        filter_by_southeast_task = PythonOperator(
            task_id = 'filter_by_southeast',
            python_callable = filter_by_southeast              
        )
        filter_by_southwest_task = PythonOperator(
            task_id = 'filter_by_southwest',
            python_callable = filter_by_southwest              
        )

    with TaskGroup('grouping') as grouping:   
        groupby_region_smoker_task = PythonOperator(
            task_id = 'groupby_region_smoker',
            python_callable = groupby_region_smoker              
        )

    

reading_and_processing>>Label('preprocessed data')>>determine_branch_task>>Label('branch on condition')>> [filtering, grouping]



