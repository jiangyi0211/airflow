This project leverages Kaggle’s Online Retail dataset and a tech stack to build an end-to-end automated retail data pipeline, focusing on data reliability and business value extraction.
Core Objectives
Automate data processing (loading, transformation, validation) to eliminate manual inefficiencies and errors.
Establish full-cycle data quality checks to ensure reliable, actionable data.
Unlock data value for retail use cases: customer behavior analysis, product sales trend tracking, and revenue structure optimization.
Key Tech Stack
Orchestration: Apache Airflow (via Astro CLI + Docker) – Automates task scheduling (data loading, transformation, validation) with DAGs for dependency management.
Storage & Computing: Google Cloud (GCS + BigQuery) – GCS stores raw/intermediate data; BigQuery (serverless warehouse) hosts structured models (dimensions/facts) and enables fast SQL analytics.
Data Transformation: dbt (via Astronomer Cosmos) – Converts raw data into analysis-ready models (e.g., dim_customer, fct_invoices) aligned with retail business logic.
Quality Assurance: Soda Core – Validates data across the pipeline (schema integrity, uniqueness, business rules like non-negative revenue) via custom checks integrated with Airflow.
Auxiliary Tools: Docker (environment consistency); Python (pandas for cleaning, SQL for querying).
Workflow
Data Ingestion: Raw dataset from Kaggle is stored in GCS.
Data Loading: Airflow syncs GCS data to BigQuery’s raw table.
Transformation: dbt builds structured models in BigQuery.
Quality Checks: Soda Core validates data at each stage; alerts on issues.
Value Output: dbt generates business reports (e.g., top 10 revenue countries, best-selling products) to support retail decision-making.
