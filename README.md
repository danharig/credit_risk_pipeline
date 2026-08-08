# Credit Risk Data Engineering Pipeline

An end-to-end data engineering project that ingests raw credit risk data, processes and transforms it using Apache Spark, stores data through an S3-compatible object storage architecture, builds analytics-ready Gold datasets, and delivers business insights through Power BI.

The project demonstrates practical data engineering concepts including **ETL development, distributed data processing, data lake architecture, data quality, dimensional aggregation, containerization, and BI integration**.

## Architecture

```text
Raw Credit Risk Dataset
          │
          ▼
    S3 Raw Layer
     (LocalStack)
          │
          ▼
    Apache Spark
          │
     Clean / Transform
          │
          ▼
   Processed Data
   (Partitioned Parquet)
          │
          ▼
      Gold Layer
  Loan Risk Aggregations
          │
          ▼
      Gold Export
          │
          ▼
       Power BI
          │
          ▼
 Credit Risk Dashboard
```

## Tech Stack

| Technology                   | Purpose                                    |
| ---------------------------- | ------------------------------------------ |
| Python                       | Pipeline development and automation        |
| Apache Spark                 | Distributed ETL and data transformation    |
| PySpark                      | DataFrame transformations and aggregations |
| Amazon S3-compatible storage | Data lake architecture                     |
| LocalStack                   | Local S3 development environment           |
| Docker                       | Reproducible Spark and storage environment |
| Parquet                      | Optimized columnar storage                 |
| Power BI                     | Analytics and dashboarding                 |
| Power Query                  | Reporting-layer transformations            |
| Git                          | Source control                             |
| GitHub                       | Repository and project documentation       |

## Pipeline Overview

### 1. Raw Data Ingestion

The pipeline begins with a raw credit risk dataset containing borrower and loan attributes.

A Python ingestion process uploads the source dataset into the raw layer of an S3-compatible data lake.

```text
s3://credit-risk-data/raw/
```

Keeping raw data separate from transformed datasets provides a reproducible starting point for downstream processing.

### 2. Spark ETL

Apache Spark processes the raw dataset using PySpark DataFrames.

The transformation layer handles data cleaning, type standardization, derived fields, and preparation of analytics-ready records.

The processed dataset is written in **Parquet format** and partitioned by loan grade.

```text
s3://credit-risk-data/processed/
    loan_grade=A/
    loan_grade=B/
    loan_grade=C/
    ...
    loan_grade=G/
```

Partitioning improves organization and demonstrates a scalable storage pattern commonly used in production data lakes.

### 3. Gold Analytics Layer

The Gold layer converts detailed loan records into business-facing credit risk metrics.

The resulting dataset contains metrics such as:

* Total loans
* Paid loans
* Defaulted loans
* Default rate
* Average loan amount
* Total loan amount
* Average interest rate
* Average borrower income
* Average loan-to-income percentage
* Risk classification
* Loan grade

Gold datasets are stored separately from raw and processed data:

```text
s3://credit-risk-data/gold/
```

This separation follows a layered data architecture in which downstream reporting tools consume curated datasets instead of performing analytics directly against raw data.

## Power BI Analytics

The Gold dataset serves as the reporting source for the Power BI dashboard.

Power Query performs lightweight presentation-layer transformations including:

* Loan grade sorting
* Risk bucket sorting
* Interest-rate buckets
* Loan-count buckets
* Loan-amount buckets
* Reporting-friendly labels and data types

The heavier data preparation and aggregation remain within the data engineering pipeline, while Power BI focuses on visualization and analytical consumption.

## Project Structure

```text
Credit_Risk_Pipeline/
│
├── Scripts/
│   ├── upload_to_s3.py
│   ├── credit_risk_etl.py
│   └── export_gold_csv.py
│
├── PowerBI/
│   └── Credit Risk Dashboard.pbix
│
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

## Local Infrastructure

The project uses Docker to create a reproducible local data engineering environment.

The environment contains:

**Apache Spark**

Executes distributed transformations and Gold-layer aggregations.

**LocalStack**

Provides an S3-compatible object storage environment, allowing the project to implement cloud-style data lake patterns locally.

This architecture allows the pipeline to demonstrate S3-based engineering workflows without requiring permanent cloud infrastructure.

## Running the Pipeline

### 1. Start the environment

```bash
docker compose up -d
```

### 2. Upload the raw dataset

```bash
python Scripts/upload_to_s3.py
```

### 3. Execute the Spark ETL pipeline

Run the Spark transformation job inside the Spark environment.

```bash
docker exec credit-risk-spark spark-submit /opt/project/Scripts/credit_risk_etl.py
```

### 4. Generate the Gold dataset

Execute the Gold-layer processing step to create analytics-ready loan risk aggregations.

### 5. Load the Gold output into Power BI

Power BI consumes the exported Gold dataset.

The report uses a Power Query parameter named:

```text
GoldCsvPath
```

Set this parameter to the location of the exported Gold CSV before refreshing the report.

This prevents machine-specific file paths from being hard-coded into the Power Query transformation logic.

## Engineering Concepts Demonstrated

This project demonstrates:

* End-to-end ETL pipeline development
* PySpark DataFrame transformations
* Distributed data processing
* S3-style data lake architecture
* Raw, processed, and Gold data layers
* Parquet storage
* Data partitioning
* Business-level aggregations
* Dockerized development environments
* Environment configuration
* Power BI integration
* Power Query parameterization
* Git-based source control

## Business Use Case

Credit risk data becomes more valuable when raw borrower and loan-level records can be converted into consistent risk metrics.

The Gold layer enables analysts and decision-makers to evaluate questions such as:

* Which loan grades have the highest default rates?
* Which loan purposes carry greater credit risk?
* How does interest rate vary across risk segments?
* Where is the largest concentration of loan exposure?
* How does borrower income relate to portfolio risk?
* Which segments should receive additional risk monitoring?

The pipeline separates these analytical concerns from raw data processing, providing a reusable dataset for downstream reporting.

## Key Takeaway

This project demonstrates how a raw dataset can be transformed into a structured analytics product through a complete data engineering workflow:

**Ingestion → Object Storage → Spark ETL → Partitioned Parquet → Gold Aggregations → Power BI**

The architecture emphasizes reproducibility, separation of data layers, scalable transformation patterns, and delivery of curated data for business intelligence.
