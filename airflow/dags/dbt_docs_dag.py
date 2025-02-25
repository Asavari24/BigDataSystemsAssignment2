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
    'dbt_docs',
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


dbt_generate_docs = BashOperator(
        task_id='generate_dbt_docs',
        bash_command= f'cd {dbt_path} && dbt docs generate --profiles-dir /opt/dbt/.dbt',
        dag=dag
    )

#dbt_serve_docs = BashOperator(
#        task_id='serve_dbt_docs',
#        bash_command= f'cd {dbt_path} && dbt docs serve --profiles-dir /opt/dbt/.dbt',
#        dag=dag
#    )


dbt_debug >> dbt_generate_docs #>> dbt_serve_docs