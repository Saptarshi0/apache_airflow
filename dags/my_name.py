from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "Owner": "saptarshi",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure":False
}

with DAG (
    dag_id = "my_name",
    description="Printing my name",
    default_args=default_args,
    start_date=datetime(2026,2,26),
    schedule_interval="@monthly",
    catchup=False,
    tags=["begining"]
) as dag:
        def print_name():
                print("Hello my name is Saptarshi")

        task_print_name = PythonOperator(
                task_id = "task_print_name",
                python_callable = print_name,
        )

        task_print_name
