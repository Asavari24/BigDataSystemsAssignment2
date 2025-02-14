**Automated SEC Financial Data Extraction & Transformation System**

**Project Summary**
This project automates the extraction, transformation, and storage of SEC Financial Statement Data Sets to support financial analysts in fundamental analysis. The pipeline is built using Apache Airflow for automation, DBT for data transformation, Snowflake for storage, and a FastAPI backend connected to a Streamlit web interface for querying financial data.

**Overview**
The system follows a structured approach:

Data Extraction → Scrape SEC datasets from the Markets Data page.
Data Storage → Process data into three formats: Raw Staging, JSON Transformation, Denormalized Fact Tables.
Data Transformation → Use DBT for schema validation, data integrity, and transformation logic.
Pipeline Automation → Implement an Airflow DAG to process, validate, and store data in Amazon S3 and Snowflake.
Data Access & UI → Develop a FastAPI backend and a Streamlit UI for interactive data retrieval and visualization.

**Key Features**
1. Data Extraction & Processing
Scrape & download SEC datasets using BeautifulSoup and custom scripts.
Parse & structure financial data for downstream processing.

2. Data Storage Strategies
Raw Staging → Store SEC data as-is for archival and auditing.
JSON Transformation → Convert structured data into JSON format for flexible querying.
Denormalized Fact Tables → Create structured Balance Sheet, Income Statement, and Cash Flow tables with key identifiers like ticker, CIK, filing date, and fiscal year.

3. Data Transformation & Validation using DBT
Define staging models for SUB, TAG, NUM, and PRE tables.
Validate schema for referential integrity and transformation correctness.
Perform data quality tests to prevent duplicate entries and incorrect mappings.

4. Data Pipeline Automation with Apache Airflow
Automate data flow from S3 to Snowflake using Airflow DAGs.
Implement job scheduling with configurations for:
Year and quarter-based job definitions
Input/output staging areas
Processing methodologies (JSON vs. RDBMS)
Ensure pipeline reusability across different datasets.

5. Post-Upload Testing & Validation
Verify data upload into Snowflake for all storage formats.
Develop automated tests to check data consistency.
Document findings and evaluate the pipeline's performance.

6. Web Interface & API Development
FastAPI Backend:
/fetch → Retrieve processed financial data from Snowflake.
/upload → Upload and process SEC datasets.
Streamlit UI:
Upload datasets and trigger processing.
Query and visualize financial data.
Evaluation & Findings
Performance Comparison
Feature	Raw Staging	JSON Transformation	Denormalized Fact Tables
Storage Efficiency	High	Moderate	Low
Query Performance	Low	Moderate	High
Scalability	High	High	Moderate
Ease of Integration	High	Moderate	High
Transformation Complexity	Low	High	High

**Key Takeaways**
Raw Staging is useful for retaining original datasets but lacks structured querying capabilities.
JSON Transformation improves accessibility but increases processing complexity.
Denormalized Fact Tables enhance query performance at the cost of storage redundancy.

**Conclusion & Next Steps**
✅ Enhance JSON Processing → Optimize JSON transformations for better structuring.
✅ Optimize Pipeline Efficiency → Improve Airflow scheduling and execution speed.
✅ Deploy for Real-World Testing → Implement in a live financial analysis environment.
✅ Assess Cost Efficiency → Compare long-term costs of different storage methods.

This project demonstrates the feasibility of automating SEC financial statement processing and provides a scalable approach for financial data management.

**Submission Requirements**
**GitHub Repository**
Includes all source code, SQL scripts, DBT models, and Airflow configurations.
GitHub Issues for tracking tasks.
Documentation & project demo video.
README.md
Repository structure, setup instructions, and usage guide.
AI Use Disclosure
Document any AI tools used in the project.

**Resources**
SEC Financial Statement Data Sets
SEC Financial Statement Converter (GitHub)
SEC Markets Data
