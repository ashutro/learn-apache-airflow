from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark():
    return SparkSession.builder \
        .appName("Airflow PySpark DAG") \
        .master("local[*]") \
        .getOrCreate()

spark = get_spark()

def read():
    df = spark.read.csv('/data-platform/data/data.csv',inferSchema = True, header=True)
    df.write.mode('overwrite').parquet('/data-platform/results/temp/')
    print("✅ CSV read and saved as Parquet")


with DAG(
    dag_id = 'daily_csv_pyspark_dag',
    start_date = datetime(2025,10,1),
    schedule_interval = "* 8 * * *",
    catchup = False
) as dag:
    
    t1 = PythonOperator(
        task_id = "read_csv",
        python_callable= read
    )

    t1