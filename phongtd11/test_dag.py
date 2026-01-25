from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def hello_world():
    """Simple task function"""
    print("Hello, Airflow!")


# Define the DAG
with DAG(
    dag_id="simple_example_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    # Define a single task
    hello_task = PythonOperator(
        task_id="hello_world_task",
        python_callable=hello_world,
    )
