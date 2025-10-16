from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

path = '/data-platform/data/data.csv'
outpath = '/data-platform/results/temp161025/'

def get_spark():
    return SparkSession.builder \
        .appName("Airflow Pyspark DAG") \
        .master("local[*]") \
        .getOrCreate()

def read():
    spark = get_spark()
    df = spark.read.csv(path, inferSchema=True, header=True)
    df.write.mode('overwrite').parquet(outpath + 'stage/')
    print("✅ CSV read and saved to stage/")
    spark.stop()

def transformation():
    spark = get_spark()
    df = spark.read.parquet(outpath + 'stage/')
    df = df.withColumn("name", F.upper(df["name"])) \
           .withColumn("city", F.upper(df["city"]))
    df.write.mode('overwrite').parquet(outpath + 'transformed/')
    print("✅ Transformation completed")
    spark.stop()

def save():
    spark = get_spark()
    df = spark.read.parquet(outpath + 'transformed/')
    df.coalesce(1).write.mode('overwrite').csv(outpath + 'final/', header=True)
    print("✅ Final CSV saved")
    spark.stop()

with DAG(
    dag_id='daily_csv_2',
    start_date=datetime(2025,10,10),
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='read_csv',
        python_callable=read
    )

    t2 = PythonOperator(
        task_id='transform_to_upper',
        python_callable=transformation
    )

    t3 = PythonOperator(
        task_id='save_parquet',
        python_callable=save
    )

    t1 >> t2 >> t3