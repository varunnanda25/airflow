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
    tags= ['upstream', 'downstream']
) as dag:

    taskA = BashOperator(
        task_id = 'taskA',
        bash_command = '''
            echo Task A has started!
            for i in {1..5}
            do
                echo Task A printing $i
            done
            echo Task A has completed!
        '''
    )

    taskB = BashOperator(
        task_id = 'taskB',
        bash_command = '''
                echo Task B has started!
                sleep 4
                echo Task B has ended!'''
    )

    taskC = BashOperator(
        task_id = 'taskC',
        bash_command = '''
                echo Task C has started!
                sleep 10
                echo Task C has ended!'''
    )

    taskD = BashOperator(
        task_id = 'taskD',
        bash_command='echo task D is executed!'
    )

taskA.set_downstream(taskB) #task b will be executed after task a
taskA.set_downstream(taskC) #task c will be executed after task a

taskD.set_upstream(taskB) #task d will be executed after task b 
taskD.set_upstream(taskC) #task d will be executed after task c