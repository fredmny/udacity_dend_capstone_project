from datetime import datetime, timedelta
import os
from airflow import DAG
from operators.process_data_spark import processDataSpark

from helpers.spark_functions import SparkFunctions

spark_functions = SparkFunctions()
input_path='s3a://fw-flights-source'
output_path='s3a://fw-flights-tables'

default_args = {
  'owner': 'fred.waldow',
  'depends_on_past': False,
  'start_date': datetime(2009, 1, 1),
  'retries': 3,
  'retry_delay': timedelta(minutes=5),
  'email_on_retry': False,
  'catchup': False,
  'catchup_by_default': False,
}

dag = DAG(
  'flights',
  default_args=default_args,
  description='Transforms data to analyze flights data',
  # schedule_interval='0 3 * * *'
)

process_airlines = processDataSpark(
  task_id='process_airlines',
  dag=dag,
  input_path=input_path,
  output_path=output_path,
  spark_function=spark_functions.process_airlines_data
)

