from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

#Define function
def start_task():
    print("Starting the task")

def processing_task():
    print("Processing data...")

def end_task():
    print("Ending the task")

#Define DAG
with DAG(
    dag_id="simple_try",
    description="A simple DAG to try out",
    start_date=datetime(2026, 3, 26),
    schedule_interval="@daily",
    catchup=False,
    tags=["begining", "example"],
) as dag:

    start_task = PythonOperator(
        task_id="start_task",
        python_callable=start_task
    )

    processing_task = PythonOperator(
        task_id="processing_task",
        python_callable=processing_task
    )

    end_task = PythonOperator(
        task_id="end_task",
        python_callable=end_task
    )

    start_task >> processing_task >> end_task
