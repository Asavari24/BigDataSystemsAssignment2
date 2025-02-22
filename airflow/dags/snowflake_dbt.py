from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


dbt_path = "/opt/dbt"
#model = "test.sql"

#dag = DAG(
#    'dbt_dag',
#    start_date=datetime(2025, 2, 13),
#    schedule_interval=None
#)


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
    'snowflake_dbt',
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

dbt_main_table = BashOperator(
    task_id='dbt_main_table',
    bash_command= f'cd {dbt_path} && dbt run --models sec_2016q4_main.sql --profiles-dir /opt/dbt/.dbt',
    dag=dag
)

dbt_bs_table = BashOperator(
    task_id='dbt_bs_table',
    bash_command= f'cd {dbt_path} && dbt run --models sec_2016q4_bs.sql --profiles-dir /opt/dbt/.dbt',
    dag=dag
)

dbt_cf_table = BashOperator(
    task_id='dbt_cf_table',
    bash_command= f'cd {dbt_path} && dbt run --models sec_2016q4_cf.sql --profiles-dir /opt/dbt/.dbt',
    dag=dag
)

dbt_ic_table = BashOperator(
    task_id='dbt_ic_table',
    bash_command= f'cd {dbt_path} && dbt run --model sec_2016q4_ic.sql --profiles-dir /opt/dbt/.dbt',
    dag=dag
)

dbt_debug >> dbt_main_table >> dbt_bs_table >> dbt_cf_table >> dbt_ic_table