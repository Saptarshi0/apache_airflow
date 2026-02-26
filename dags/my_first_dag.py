from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "saptarshi",
    "retries": 1,                            # Retry once if task fails
    "retry_delay": timedelta(minutes=5),     # Wait 5 min before retry
    "email_on_failure": False,
}

with DAG(
    dag_id="my_first_dag",                   # Unique name shown in UI
    description="My very first Airflow DAG",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),         # When scheduling starts
    schedule_interval="@daily",              # Run once a day
    catchup=False,                           # Don't backfill past runs
    tags=["learning", "beginner"],           # Helps filter in UI
) as dag:
    
    task_start = BashOperator(
        task_id="task_start",
        bash_command='echo "🚀 DAG has started! Date: $(date)"',
    )

    def say_hello():
        print("Hello from Task 2!")
        print("This is a PythonOperator task.")
        return "Hello returned successfully"

    task_hello = PythonOperator(
        task_id="task_hello",
        python_callable=say_hello,
    )

    def print_run_info(**context):
        print(f"📅 Execution Date : {context['execution_date']}")
        print(f"🔁 DAG Run ID     : {context['run_id']}")
        print(f"📌 Task Instance  : {context['task_instance_key_str']}")

    task_info = PythonOperator(
        task_id="task_run_info",
        python_callable=print_run_info,
    )

    task_end = BashOperator(
        task_id="task_end",
        bash_command='echo "✅ DAG completed successfully!"',
    )
    task_start >> task_hello >> task_info >> task_end