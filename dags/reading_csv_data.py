from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import os
from pyspark.sql import SparkSession


FILE_PATH = "/opt/airflow/data/employees.csv"


# 🔹 Task 1: Read CSV
def read_csv(**context):
    if not os.path.exists(FILE_PATH):
        raise FileNotFoundError(f"File not found at {FILE_PATH}")

    spark = SparkSession.builder.appName("Read CSV").getOrCreate()

    df = spark.read.csv(FILE_PATH, header=True, inferSchema=True)

    logging.info("✅ CSV loaded successfully")

    # Convert to JSON (XCom-friendly)
    data = df.limit(100).toJSON().collect()

    spark.stop()

    return data


# 🔹 Task 2: Log first 10 rows
def log_data(**context):
    data = context["ti"].xcom_pull(task_ids="read_csv_task")

    logging.info("🔍 First 10 rows:")

    for row in data[:10]:
        logging.info(row)


# 🔹 Task 3: Aggregation (example)
def aggregate_data(**context):
    data = context["ti"].xcom_pull(task_ids="read_csv_task")

    spark = SparkSession.builder.appName("Aggregation").getOrCreate()

    df = spark.read.json(spark.sparkContext.parallelize(data))

    # Example aggregation: count total rows
    total_count = df.count()

    logging.info("📊 Total row count: %s", total_count)

    spark.stop()


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}


with DAG(
    dag_id="reading_csv_data_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    read_csv_task = PythonOperator(
        task_id="read_csv_task",
        python_callable=read_csv
    )

    log_data_task = PythonOperator(
        task_id="log_data_task",
        python_callable=log_data
    )

    aggregate_task = PythonOperator(
        task_id="aggregate_task",
        python_callable=aggregate_data
    )

    # Task dependencies
    read_csv_task >> log_data_task >> aggregate_task