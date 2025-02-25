from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


dbt_path = "/opt/dbt"


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1)
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}


# Create DAG
dag = DAG(
    'dbt_test',
    default_args=default_args,
    description='Snowflake data transformation with dbt',
    schedule_interval=None,
    catchup=False
)

dbt_debug = BashOperator(
    task_id='dbt_debug',
    bash_command= f'cd {dbt_path} && dbt debug --profiles-dir /opt/dbt/.dbt',
    dag=dag
)

dbt_test = BashOperator(
        task_id='run_dbt_tests',
        bash_command= f'cd {dbt_path} && dbt test --profiles-dir /opt/dbt/.dbt',
        dag=dag
    )


dbt_debug >> dbt_test