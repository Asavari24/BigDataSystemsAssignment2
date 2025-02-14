FROM apache/airflow:2.10.5
USER airflow
RUN pip install dbt-snowflake 

USER root
RUN sudo apt-get update
RUN sudo apt install git -y