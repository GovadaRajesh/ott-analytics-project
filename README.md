# ott-analytics-project
End-to-end OTT Analytics Data Pipeline using Airflow, Dataflow, GCS, and BigQuery

# OTT Analytics Data Pipeline

This project implements an end-to-end data engineering pipeline for OTT view log analytics using
Airflow, Google Cloud Dataflow, GCS, and BigQuery.

---

## Architecture
CSV → GCS → Dataflow → BigQuery (Bronze → Silver → Gold) → Analytics

---

## Tech Stack
- Apache Airflow (Orchestration)
- Google Cloud Storage
- Dataflow (Apache Beam - Python)
- BigQuery
- SQL
- Python

---

## Project Structure
ott-analytics-project/
│
├── dags/
│   └── ott_analytics_production.py
│
├── dataflow/
│   └── ott_dataflow_pipeline.py
│
├── sql/
│   ├── bronze_create.sql
│   ├── silver_transform.sql
│   └── gold_aggregation.sql
│
├── gcs/
│   └── ott-raw-data/
│
├── bigquery/
│   ├── ott_bronze/
│   ├── ott_silver/
│   └── ott_gold/
│
└── README.md


---

## Pipeline Flow
1. Upload raw CSV to GCS
2. Airflow triggers Dataflow job
3. Dataflow loads data into Bronze
4. Silver cleans & transforms data
5. Gold aggregates metrics
6. Final validation in BigQuery

---

## How to Run
```bash
airflow dags trigger ott_analytics_pipeline



