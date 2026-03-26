from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "bash_ops_dag",
    description = "A DAG to test bash operators",
    start_date = datetime(2026, 3, 26),
    schedule_interval = "@daily",
    catchup = False,
    tags = ["begining", "example"],
) as dag:

    #task to print the current date
    print_date = BashOperator(
        task_id = "print_date",
        bash_command = "date"
    )

    #list the files in the current directory
    list_files = BashOperator(
        task_id = "list_files",
        bash_command = "ls -l /opt/airflow/dags"
    )

    #create a new file
    create_file = BashOperator(
        task_id = "create_file",
        bash_command = "echo 'This is a test file' > /opt/airflow/dags/test_file.txt"
    )

    #print the contents of the file
    print_file = BashOperator(
        task_id = "print_file",
        bash_command = "cat /opt/airflow/dags/test_file.txt"
    )

    #set dependencies
    print_date >> list_files >> create_file >> print_file
