from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
   'owner' : 'loonycorn'
}

with DAG(
    dag_id = 'executing_multiple_tasks',
    description = 'DAG with multiple tasks and dependencies',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = timedelta(days=1),
    tags= ['upstream', 'downstream'],
    template_searchpath=['/home/varun/airflow/dags/bash scripts/']
    
) as dag:

    taskA = BashOperator(
        task_id = 'taskA',
        bash_command = 'taskA.sh'
    )

    taskB = BashOperator(
        task_id = 'taskB',
        bash_command = 'taskB.sh'
    )

    taskC = BashOperator(
        task_id = 'taskC',
        bash_command = 'taskC.sh'
    )

    taskD = BashOperator(
        task_id = 'taskD',
        bash_command='taskD.sh'
    )

    taskE = BashOperator(
        task_id = 'taskE',
        bash_command='taskE.sh'
    )
    taskF = BashOperator(
        task_id = 'taskF',
        bash_command='taskF.sh'
    )

#taskA >> [taskB, taskC] #task b and task c will be executed after task a  using bit shift operators

#taskD << [taskB, taskC] #task d will be executed after task c and task b using bit shift operators

taskA >> taskB >> taskE
taskA >> taskC >> taskF
taskA >> taskD 