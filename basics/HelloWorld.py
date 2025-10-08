from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def task1():
    print("Hello Airflow")
def task2():
    print("Task 2 completed")


with DAG(
    dag_id = "first_dag",
    start_date = datetime(2025,10,1),
    schedule_interval = "@daily",
    catchup = False
) as dag:
    t1 = PythonOperator(
        task_id = "print_hello",
        python_callable=task1
    )

    t2 = PythonOperator(
        task_id  = "print_task2",
        python_callable=task2
    )

    t1 >> t2