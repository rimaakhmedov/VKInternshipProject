from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as F
from datetime import datetime, timedelta
import pandas as pd
import os
import re


NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
TODAY = datetime.now().strftime('%Y-%m-%d')

def count_actions():
    print(f"{NOW} Start spark session\n")

    spark = SparkSession \
        .builder \
        .config("spark.master", "local") \
        .config('spark.executor.cores', '4') \
        .config('spark.executor.memory', '2g') \
        .config('spark.driver.memory', '2g') \
        .config('spark.dynamicAllocation.minExecutors', '4') \
        .appName("spark_count") \
        .getOrCreate()

    print(f"{NOW} Spark is started\n")
    print(f"{NOW} ===> Spark UI: {spark.sparkContext.uiWebUrl} <===\n")

    input_date = datetime.strptime(TODAY, "%Y-%m-%d")
    date_list = [(input_date - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, 8)]
    file_path_pattern = f"input/{{date}}.csv"

    combined_df = None

    for date_str in date_list:
        file_path = file_path_pattern.format(date=date_str)
        try:
            df = spark.read.format("csv") \
                .option("header", "false") \
                .option("inferSchema", "true") \
                .load(file_path) \
                .toDF("email", "action", "timestamp")  # Указываем имена колонок

            if combined_df is None:
                combined_df = df
            else:
                combined_df = combined_df.union(df)
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")

    if combined_df is not None:
        result_df = combined_df.groupBy("email").agg(
            F.sum(F.when(combined_df["action"] == "CREATE", 1).otherwise(0)).alias("create_count"),
            F.sum(F.when(combined_df["action"] == "READ", 1).otherwise(0)).alias("read_count"),
            F.sum(F.when(combined_df["action"] == "UPDATE", 1).otherwise(0)).alias("update_count"),
            F.sum(F.when(combined_df["action"] == "DELETE", 1).otherwise(0)).alias("delete_count")
        )

        result_df.toPandas().to_csv(f'output/{TODAY}.csv')
    else:
        print("The files for the specified dates could not be read.")

    # Останавливаем Spark сессию
    spark.stop()
    print(f"{TODAY}.csv created!\n\n")

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1
    }

dag = DAG(
    dag_id="spark_count",
    default_args=default_args,
    schedule_interval='0 7 * * *'
)


run_spark = PythonOperator(
    task_id='run_spark_count',
    python_callable=count_actions,
    dag=dag
)


run_spark