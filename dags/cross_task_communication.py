from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
   'owner' : 'varun25'
}
def increment_by_2(counter):
    print("count {counter}".format(counter=counter))
    return counter+2

def multiply_by_2(counter):
    print("count {counter}".format(counter=counter))
    return counter*2


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
        op_kwargs={'counter':100} # specifying argument for the function
    )
    taskB=PythonOperator(
        task_id='multiply_by_2',
        python_callable=multiply_by_2,
        op_kwargs={'counter':10} # specifying arguments for the function
    )

taskA>>taskB