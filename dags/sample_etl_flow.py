from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Default arguments ───────────────────────────────
default_args = {
    "owner": "saptarshi",
    "retries": 1,
}

# ── Define the DAG ──────────────────────────────────
with DAG(
    dag_id="etl_example_dag",
    description="A simple ETL workflow",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # ── Task 1: Extract ─────────────────────────────
    def extract_data():
        print("📥 Extracting data... (pretend we pulled from a database)")
        return {"name": "Saptarshi", "age": 30}

    task_extract = PythonOperator(
        task_id="extract_task",
        python_callable=extract_data,
    )

    # ── Task 2: Transform ───────────────────────────
    def transform_data(**context):
        data = context["ti"].xcom_pull(task_ids="extract_task")
        data["age"] += 1  # pretend transformation
        print(f"🔧 Transformed data: {data}")
        return data

    task_transform = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
    )

    # ── Task 3: Load ────────────────────────────────
    def load_data(**context):
        data = context["ti"].xcom_pull(task_ids="transform_task")
        print(f"💾 Loading data: {data} (pretend saving to warehouse)")

    task_load = PythonOperator(
        task_id="load_task",
        python_callable=load_data,
    )

    # ── Dependencies ────────────────────────────────
    task_extract >> task_transform >> task_load
