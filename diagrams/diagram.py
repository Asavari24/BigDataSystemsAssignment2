from diagrams import Diagram, Cluster, Edge
from diagrams.aws.storage import S3
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.compute import Server
from diagrams.onprem.analytics import Dbt
from diagrams.onprem.container import Docker
from diagrams.programming.language import Python
from diagrams.onprem.client import Users
from diagrams.saas.analytics import Snowflake

with Diagram("SEC Data Pipeline Architecture", show=False, direction="LR"):
    
    with Cluster("Web Scraping"):
        sec_website = Server("SEC Website")
        scraper = Python("Scraper")
        sec_website >> Edge(label="Scrapes text files") >> scraper
    
    with Cluster("AWS S3 Storage"):
        s3_raw = S3("Raw Text Files")
        s3_json = S3("JSON Files")
        scraper >> Edge(label="Stores raw text") >> s3_raw
        s3_raw >> Edge(label="Convert text to JSON") >> s3_json
    
    with Cluster("Snowflake"):
        snowflake_stage = Snowflake("Stage: JSON Files")
        snowflake_tables = Snowflake("Structured Tables")
        s3_json >> Edge(label="Linked to Snowflake") >> snowflake_stage
    
    with Cluster("Data Transformation"):
        dbt = Dbt("dbt Transformation")
        snowflake_stage >> Edge(label="Transform JSON to Tables") >> dbt
        dbt >> Edge(label="Store structured data") >> snowflake_tables
    
    with Cluster("Data Orchestration"):
        airflow = Airflow("Airflow Scheduler")
        airflow >> Edge(label="Schedules entire pipeline") >> [scraper, dbt]
    
    with Cluster("Deployment on EC2"):
        ec2 = Server("EC2 Instance")
        fastapi = Python("FastAPI")
        streamlit = Python("Streamlit")
        ec2 >> Edge(label="Hosts FastAPI") >> fastapi
        ec2 >> Edge(label="Hosts Streamlit") >> streamlit
    
    with Cluster("User Interaction"):
        users = Users("End Users")
        streamlit >> Edge(label="Shows data visualization") >> users
    
    fastapi >> Edge(label="Fetch structured data") >> snowflake_tables
