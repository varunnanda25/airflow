from airflow import DAG

from airflow.providers.sqlite.operators.sqlite import SqliteOperator

from datetime import date, datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner' : 'varun25'
}

with DAG(
    dag_id = 'executing_sql_pipeline',
    description = 'Pipeline using SQL operators',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['pipeline', 'sql']
) as dag:
    
    create_table = SqliteOperator(
        task_id = 'create_table',
        sql = r"""
            CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(50) NOT NULL,
                    age INTEGER NOT NULL,
                    city varchar (50),
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )

    insert_values_1=SqliteOperator(
        task_id = 'insert_values_1',
        sql=r"""
            INSERT INTO users (name, age, is_active) VALUES
            ('Alice', 30, false),
            ('Bob', 25, true),
            ('Charlie', 35, false),
            ('Joseph', 28, true);""",
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )
    insert_values_2=SqliteOperator(
        task_id = 'insert_values_2',
        sql=r"""
            INSERT INTO users (name, age) VALUES
            ('David', 40),
            ('Eve', 22),
            ('Frank', 29),
            ('Grace', 31);""",
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )
    delete_values=SqliteOperator(
        task_id = 'delete_values',
        sql=r"""
            DELETE FROM users WHERE is_active=false;
            """,
            sqlite_conn_id='my_sqlite_conn',
            dag=dag,
    )

    update_values=SqliteOperator(
        task_id = 'update_values',
        sql=r"""
            UPDATE users SET city='New York';
            """,   
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    ) 

    display_results=SqliteOperator(
        task_id = 'display_results',
        sql=r"""
            SELECT * FROM users;
            """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
        do_xcom_push = True
    )

create_table >>[insert_values_1, insert_values_2] >> delete_values >> update_values>> display_results

