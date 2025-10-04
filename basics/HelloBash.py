from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


def say_hello():
    print("Hellow Airflow from Python")

with DAG(
    dag_id="first_bash",
    start_date = datetime(2025,10,1),
    schedule_interval = '@daily',
    catchup = False
) as dag:

    t1 = PythonOperator(
        task_id="hello_python",
        python_callable = say_hello
    )

    t2 = BashOperator(
        task_id = "hello_bas",
        bash_command='echo "Hello Airflow from bash"'
    )

    t1 >> t2