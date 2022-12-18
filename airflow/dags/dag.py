from datetime import datetime, timedelta
import os
from airflow import DAG
from operators import

from helpers import SparkFunctions

spark_functions = SparkFunctions()

default_args = {
  'owner': 'fred.waldow',
  'depends_on_past': False,
  'start_date': datetime(2009, 01, 01),
  'retries': 3,
  'retry_delay': timedelta(minutes=5),
  'email_on_retry': False,
  'catchup': False
}

dag = DAG(
  'flights',
  default_args=default_args,
  description='Transforms data to analyze flights data',
  schedule_interval='0 3 * * *'
)

