**#Automated Financial Data Extraction & Transformation System - Assignment 2**
Project Summary
This project focuses on automating the extraction, transformation, and storage of financial statement data from the SEC Financial Statement Data Sets. The goal is to develop a fully functional data pipeline using Apache Airflow that extracts, processes, and loads financial data into Snowflake for analysis. The extracted data is transformed into multiple formats, including raw staging, JSON transformation, and denormalized fact tables, to optimize storage and retrieval efficiency.

Overview
The project explores the feasibility of different data storage strategies and transformation techniques to support financial analysts in conducting fundamental analysis of US public companies. The end-to-end pipeline integrates multiple technologies:

Data Extraction: Scraping SEC financial statement datasets
Data Storage: Implementing raw staging, JSON transformation, and denormalized fact tables
Data Transformation: Using DBT for schema validation and transformation
Operational Pipeline: Automating data processing with Apache Airflow
Data Access: Developing a FastAPI backend and a Streamlit user interface for querying financial data
Key Features
1. Data Extraction & Processing
SEC Data Scraping
Scrape dataset links from the SEC Markets Data page.
Parse financial statement data from structured SEC datasets.
2. Data Storage Design
Raw Staging: Store SEC data in its original format.
JSON Transformation: Convert structured data into JSON format using SecFinancialStatementConverter for flexible querying.
Denormalized Fact Tables: Structure SEC data into three denormalized tables:
Balance Sheet
Income Statement
Cash Flow Statement
Each table includes essential identifiers such as ticker, CIK, filing date, fiscal year, and period.

3. Data Transformation & Validation using DBT
Implement DBT models for validating and transforming data.
Ensure schema validation, referential integrity, and transformation accuracy.
Perform data quality tests on the transformed tables.
Document transformation strategies and testing methodologies.
4. Data Pipeline Automation with Apache Airflow
Pipeline Components:
Extract, validate, and store datasets in S3 for intermediate processing.
Process data using JSON or RDBMS storage approaches.
Automate scheduling and execution with Apache Airflow.
Pipeline Configurations:
Year and quarter-based job definitions
Input/output staging areas (S3 and Snowflake)
Data validation and transformation methodologies
5. Data Upload Testing
Develop and execute tests for ensuring data integrity.
Validate data uploads across raw staging, JSON, and denormalized fact tables.
Document findings and provide insights into pipeline efficiency.
6. Web Interface & API Development
FastAPI Backend:
/fetch API: Retrieves processed financial data from Snowflake.
/upload API: Uploads and processes SEC datasets based on selected format (raw, JSON, or RDBMS).
Streamlit Interface:
Users upload datasets and trigger processing.
Provides interactive data visualization and downloadable reports.
Evaluation & Findings
Performance Comparison
Feature	Raw Staging	JSON Transformation	Denormalized Fact Tables
Storage Efficiency	High	Moderate	Low
Query Performance	Low	Moderate	High
Scalability	High	High	Moderate
Ease of Integration	High	Moderate	High
Transformation Complexity	Low	High	High
Key Takeaways
Raw Staging is useful for retaining original datasets but lacks structured querying capabilities.
JSON Transformation improves accessibility and allows for more flexible queries but increases processing complexity.
Denormalized Fact Tables enhance query performance at the cost of storage redundancy.
Conclusion & Next Steps
Enhance JSON Processing: Optimize JSON transformations for better data structuring.
Optimize Pipeline Performance: Improve Airflow scheduling and execution efficiency.
Deploy for Real-World Testing: Implement the system in a live financial analysis environment.
Assess Cost Efficiency: Compare long-term costs of different storage methods.
This project demonstrates the feasibility of automating SEC financial statement processing and provides a scalable approach for financial data management.

Submission Requirements
GitHub Repository
Includes all source code, SQL scripts, DBT models, and Airflow configurations.
GitHub Issues for task tracking.
Documentation and a project overview video.
README.md
Repository structure, setup instructions, and usage guide.
AI Use Disclosure
Documenting AI tools used (if applicable).
Resources
SEC Financial Statement Data Sets
SEC Financial Statement Converter (GitHub)
SEC Markets Data
