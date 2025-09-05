from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
   'owner' : 'varun25'
}
def increment_by_2(value):
    print("value {value}".format(value=value))
    return value+2

def multiply_by_2(ti):
    value=ti.xcom_pull(task_ids='increment_by_2')
    print("value {value}".format(value=value))
    return value*2

def subtract_2(ti):
    value=ti.xcom_pull(task_ids='multiply_by_2')
    print("value {value}".format(value=value))
    return value-2

def print_value(ti):
    value=ti.xcom_pull(task_ids='subtract_2')
    print("value from xcom ti {value}".format(value=value))


with DAG(
    dag_id='cross_task_communication',
    description='Cross Task communication using XCom',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=timedelta(days=1),
    tags=['python', 'Xcom']
) as dag:
    taskA=PythonOperator(
        task_id='increment_by_2',
        python_callable=increment_by_2,
        op_kwargs={'value':100} # specifying argument for the function
    )
    taskB=PythonOperator(
        task_id='multiply_by_2',
        python_callable=multiply_by_2,
    )
    taskC=PythonOperator(
        task_id='subtract_2',
        python_callable=subtract_2,
    )
    taskD=PythonOperator(
        task_id='print_value',
        python_callable=print_value,
    )

taskA>>taskB >>taskC>>taskD