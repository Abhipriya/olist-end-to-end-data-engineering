# Olist End-to-End Data Engineering Pipeline

An end-to-end data engineering project built using the Brazilian Olist e-commerce dataset.

## Architecture

CSV
→ Apache Airflow
→ Parquet
→ Snowflake RAW
→ dbt STAGING
→ dbt Tests
→ dbt ANALYTICS

Infrastructure is provisioned using Terraform and the environment runs using Docker.

## Tech Stack

- Apache Airflow 3.3.1
- DAG Factory
- Docker
- Python
- Pandas
- PyArrow
- Snowflake
- dbt Core
- dbt-snowflake
- Terraform
- Git

## Pipeline

### 1. Dataset Discovery
Airflow dynamically discovers datasets from the landing directory.

### 2. CSV → Parquet
CSV datasets are converted to Parquet using Pandas and PyArrow.

### 3. Snowflake Stage
Parquet files are uploaded to an internal Snowflake stage.

### 4. RAW Layer
Snowflake tables are inferred and populated from the Parquet files.

### 5. STAGING Layer
dbt cleans and standardizes RAW data.

### 6. Data Quality
dbt tests validate keys, nullability, uniqueness, and relationships.

### 7. ANALYTICS Layer
dbt builds dimensional and fact tables for analytics.

## Analytics Models

### Dimensions

- DIM_CUSTOMERS
- DIM_PRODUCTS

### Facts

- FCT_ORDERS
- FCT_ORDER_ITEMS
- FCT_PAYMENTS

## Orchestration

The Airflow DAG runs:

discover_datasets
→ csv_to_parquet
→ upload_parquet_to_stage
→ create_raw_table
→ copy_parquet_to_raw
→ validate_raw_load
→ dbt_run_staging
→ dbt_test_staging
→ dbt_run_marts
→ dbt_test_marts

## Security

Snowflake authentication uses RSA key-pair authentication.

Secrets, Terraform state, environment files, and dbt profiles are excluded from Git.