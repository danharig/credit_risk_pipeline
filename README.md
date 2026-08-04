# Credit Risk Data Pipeline with Apache Spark, LocalStack, Docker & Power BI

## Overview

This project demonstrates an end-to-end data engineering pipeline for processing and analyzing credit risk data using Apache Spark, Docker, LocalStack (AWS S3 emulation), Python, and Power BI.

The pipeline ingests raw loan data into an S3-compatible object store, performs ETL transformations with PySpark, creates curated analytical datasets, and visualizes business insights through an interactive Power BI dashboard.

---

## Architecture

Raw CSV
⬇
LocalStack S3 (Bronze)
⬇
PySpark ETL
⬇
Partitioned Parquet (Silver)
⬇
PySpark Aggregations
⬇
Gold Dataset
⬇
Power BI Dashboard

---

## Tech Stack

- Python
- Apache Spark (PySpark)
- Docker
- LocalStack (AWS S3 Emulation)
- Apache Parquet
- Power BI
- Git
- GitHub
- boto3

---

## Project Structure

```text
Credit_Risk_Pipeline/
│
├── Documentation/
│   └── Credit_Risk_Pipeline_Portfolio_Overview.docx
│
├── PowerBI/
│   ├── Dashboard/
│   │   └── Credit Risk Dashboard.pbix
│   └── CreditRiskGold/
│
├── Scripts/
│   ├── upload_to_s3.py
│   ├── credit_risk_etl.py
│   ├── credit_risk_gold.py
│   └── export_gold_csv.py
│
├── docker-compose.yml
├── requirements.txt
├── README.md
└── .gitignore
```

---

# Pipeline Workflow

## 1. Upload Raw Dataset

The raw CSV is uploaded into a LocalStack S3 bucket using boto3.

Responsibilities:

- Creates the bucket if it does not exist
- Uploads the raw dataset
- Mimics an AWS S3 ingestion process locally

Technology:

- Python
- boto3
- LocalStack

---

## 2. ETL Processing

PySpark reads the raw dataset from LocalStack and performs data cleaning and feature engineering.

Transformations include:

- Remove invalid ages
- Fill missing employment length
- Fill missing interest rates
- Create loan status labels
- Create risk buckets
- Partition output by loan grade

Output:

Silver Layer (Partitioned Parquet)

---

## 3. Gold Layer

The Silver dataset is aggregated into a business-ready Gold dataset.

Examples include:

- Loan Counts
- Total Loan Amount
- Average Interest Rate
- Average Income
- Default Rate
- Risk Distribution

---

## 4. Export

The Gold dataset is exported as a CSV for Power BI reporting.

---

# Power BI Dashboard

The dashboard provides executive-level insights into the loan portfolio.

Key KPIs

- Total Loan Amount
- Total Loan Count
- Average Interest Rate
- Default Rate
- Average Annual Income

Interactive Visuals

- Scatter Plot
- Risk Distribution
- Loan Grade Analysis
- Loan Purpose Breakdown
- Income Bucket Analysis
- Matrix Summary
- Dynamic Slicers

Users can filter by:

- Loan Purpose
- Loan Grade
- Income Bucket
- Risk Bucket

---

# Example Business Insights

The dashboard can answer questions such as:

- Which loan grades have the highest default rates?
- How does annual income impact default risk?
- Which loan purposes carry the largest balances?
- What percentage of loan dollars fall into each risk category?
- Which customer segments represent the highest credit exposure?

---

# Running the Project

## Start Docker

```bash
docker compose up -d
```

## Upload the dataset

```bash
python Scripts/upload_to_s3.py
```

## Run the ETL

```bash
python Scripts/credit_risk_etl.py
```

## Build the Gold Layer

```bash
python Scripts/credit_risk_gold.py
```

## Export Gold Dataset

```bash
python Scripts/export_gold_csv.py
```

---

# Skills Demonstrated

### Data Engineering

- ETL Pipeline Design
- Apache Spark
- PySpark DataFrames
- Data Cleaning
- Feature Engineering
- Partitioned Parquet
- Object Storage

### Cloud

- AWS S3 Concepts
- LocalStack
- Docker Containers

### Analytics

- Power BI
- KPI Reporting
- Interactive Dashboards
- Business Intelligence

### Software Development

- Python
- Git
- GitHub
- Modular Code Design

---

# Future Improvements

- Apache Airflow orchestration
- AWS S3 deployment
- AWS Glue integration
- Amazon Athena querying
- CI/CD pipeline with GitHub Actions
- Automated data quality validation
- Unit testing
- Infrastructure as Code (Terraform)

---

# Author

**Dan Harig**

Business Intelligence Analyst | Data Engineering Portfolio

Technologies:

Python • SQL • PySpark • Docker • Power BI • AWS • LocalStack • Git
