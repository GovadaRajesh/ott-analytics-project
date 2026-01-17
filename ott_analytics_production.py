from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

# =========================================================
# CONFIG
# =========================================================
PROJECT_ID = "koti-481516"
REGION = "us-central1"
BQ_LOCATION = "US"

RAW_BUCKET = "ott-raw-k-data"
DATAFLOW_BUCKET = "ott-dataflow-k-temp"
CODE_BUCKET = "ott-code-k"

BRONZE_DS = "ott_bronze"
SILVER_DS = "ott_silver"
GOLD_DS = "ott_gold"

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
}

# =========================================================
# DAG
# =========================================================
with DAG(
    dag_id="ott_analytics_production",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ott", "dataflow", "bigquery"],
) as dag:

    # 1️ CREATE BIGQUERY DATASETS
    create_bronze_ds = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bronze_dataset",
        project_id=PROJECT_ID,
        dataset_id=BRONZE_DS,
        location=BQ_LOCATION,
        exists_ok=True,
    )

    create_silver_ds = BigQueryCreateEmptyDatasetOperator(
        task_id="create_silver_dataset",
        project_id=PROJECT_ID,
        dataset_id=SILVER_DS,
        location=BQ_LOCATION,
        exists_ok=True,
    )

    create_gold_ds = BigQueryCreateEmptyDatasetOperator(
        task_id="create_gold_dataset",
        project_id=PROJECT_ID,
        dataset_id=GOLD_DS,
        location=BQ_LOCATION,
        exists_ok=True,
    )

    # 2️ WAIT FOR INPUT FILE
    check_file_exists = GCSObjectExistenceSensor(
        task_id="check_csv_file_exists",
        bucket=RAW_BUCKET,
        object="view_log_{{ ds }}.csv",
        mode="reschedule",
        poke_interval=60,
        timeout=1800,
    )

    # 3 ENSURE BRONZE TABLE EXISTS (BEFORE DATAFLOW)
    create_bronze_table = BigQueryInsertJobOperator(
        task_id="create_bronze_table",
        location=BQ_LOCATION,
        configuration={
            "query": {
                "query": f"""
                CREATE TABLE IF NOT EXISTS
                `{PROJECT_ID}.{BRONZE_DS}.view_logs` (
                    user_id STRING,
                    show_id STRING,
                    device_type STRING,
                    duration_minutes INT64,
                    view_timestamp TIMESTAMP
                )
                """,
                "useLegacySql": False,
            }
        },
    )

    # 4️ DATAFLOW → BRONZE
    run_dataflow_bronze = BeamRunPythonPipelineOperator(
        task_id="run_dataflow_bronze",
        py_file=f"gs://ott-code-k/ott_dataflow_pipeline.py",
        runner="DataflowRunner",
        pipeline_options={
            "project": PROJECT_ID,
            "region": REGION,
            "temp_location": f"gs://ott-dataflow-k-temp/temp",
            "staging_location": f"gs://ott-dataflow-k-temp/staging",
            "input": f"gs://ott-raw-k-data/view_log_{{{{ ds }}}}.csv",
            "output_table": f"{PROJECT_ID}:{BRONZE_DS}.view_logs",
        },
    )

    # 5️ VALIDATE BRONZE (LIGHT CHECK)
    check_bronze = BigQueryCheckOperator(
        task_id="validate_bronze_data",
        sql=f"SELECT 1 FROM `{PROJECT_ID}.{BRONZE_DS}.view_logs` LIMIT 1",
        use_legacy_sql=False,
        location=BQ_LOCATION,
    )

    # 6️ SILVER TRANSFORMATION
    silver_transform = BigQueryInsertJobOperator(
        task_id="silver_transform",
        location=BQ_LOCATION,
        configuration={
            "query": {
                "query": """
                CREATE TABLE IF NOT EXISTS
                  `{{ params.project }}.{{ params.silver }}.view_logs_clean`
                (
                    user_id STRING,
                    show_id STRING,
                    device_type STRING,
                    duration_minutes INT64,
                    view_date DATE
                )
                PARTITION BY view_date;

                INSERT INTO `{{ params.project }}.{{ params.silver }}.view_logs_clean`
                SELECT DISTINCT
                    user_id,
                    show_id,
                    device_type,
                    duration_minutes,
                    DATE(view_timestamp)
                FROM `{{ params.project }}.{{ params.bronze }}.view_logs`
                WHERE duration_minutes IS NOT NULL;
                """,
                "useLegacySql": False,
            }
        },
        params={
            "project": PROJECT_ID,
            "bronze": BRONZE_DS,
            "silver": SILVER_DS,
        },
    )

    # 7️ GOLD AGGREGATION
    gold_transform = BigQueryInsertJobOperator(
        task_id="gold_aggregation",
        location=BQ_LOCATION,
        configuration={
            "query": {
                "query": """
                CREATE TABLE IF NOT EXISTS
                  `{{ params.project }}.{{ params.gold }}.daily_show_metrics`
                (
                    show_id STRING,
                    view_date DATE,
                    unique_users INT64,
                    total_watch_minutes INT64,
                    avg_watch_minutes FLOAT64,
                    updated_at TIMESTAMP
                )
                PARTITION BY view_date;

                MERGE `{{ params.project }}.{{ params.gold }}.daily_show_metrics` t
                USING (
                    SELECT
                        show_id,
                        view_date,
                        COUNT(DISTINCT user_id) AS unique_users,
                        SUM(duration_minutes) AS total_watch_minutes,
                        AVG(duration_minutes) AS avg_watch_minutes,
                        CURRENT_TIMESTAMP() AS updated_at
                    FROM `{{ params.project }}.{{ params.silver }}.view_logs_clean`
                    GROUP BY show_id, view_date
                ) s
                ON t.show_id = s.show_id AND t.view_date = s.view_date
                WHEN MATCHED THEN UPDATE SET
                    unique_users = s.unique_users,
                    total_watch_minutes = s.total_watch_minutes,
                    avg_watch_minutes = s.avg_watch_minutes,
                    updated_at = s.updated_at
                WHEN NOT MATCHED THEN INSERT
                VALUES (
                    s.show_id, s.view_date, s.unique_users,
                    s.total_watch_minutes, s.avg_watch_minutes, s.updated_at
                );
                """,
                "useLegacySql": False,
            }
        },
        params={
            "project": PROJECT_ID,
            "silver": SILVER_DS,
            "gold": GOLD_DS,
        },
    )

    # 8 FINAL VALIDATION
    validate_gold = BigQueryCheckOperator(
        task_id="validate_gold_metrics",
        sql=f"SELECT 1 FROM `{PROJECT_ID}.{GOLD_DS}.daily_show_metrics` LIMIT 1",
        use_legacy_sql=False,
        location=BQ_LOCATION,
    )

    # =====================================================
    # DAG DEPENDENCIES
    # =====================================================
    [create_bronze_ds, create_silver_ds, create_gold_ds] >> check_file_exists
    check_file_exists >> create_bronze_table >> run_dataflow_bronze
    run_dataflow_bronze >> check_bronze >> silver_transform
    silver_transform >> gold_transform >> validate_gold
