from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

# Parameters\
spark_app_name = "Flights ETL"
config_file_path = "/usr/local/spark/resources/data/airflow.cfg"
input_path='s3a://fw-flights-source'
output_path='s3a://fw-flights-tables'

# DAG Definition
now = datetime.now()

default_args = {
    "owner": "fred.waldow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "catchup_by_default": False,
}

dag = DAG(
        "flights", 
        default_args=default_args, 
        description='Transforms data to analyze flights data',
        schedule_interval='0 3 * * *'
    )

start = DummyOperator(task_id="start", dag=dag)

process_airlines = SparkSubmitOperator(
    task_id="process_airline_data",
    dag=dag,
    application="/home/user/airflow/spark/app/process_airline_data.py",
    name=spark_app_name,
    conn_id="spark_local",
    verbose=1,
    conf={"spark.master":"local[*]"},
    application_args=[config_file_path]
)

process_airports = SparkSubmitOperator(
    task_id="process_airport_data",
    dag=dag,
    application="/home/user/airflow/spark/app/process_airport_data.py",
    name=spark_app_name,
    conn_id="spark_local",
    verbose=1,
    conf={"spark.master":"local[*]"},
    application_args=[config_file_path]
)

process_flights = SparkSubmitOperator(
    task_id="process_flights_data",
    dag=dag,
    application="/home/user/airflow/spark/app/process_flights_data.py",
    name=spark_app_name,
    conn_id="spark_local",
    verbose=1,
    conf={"spark.master":"local[*]"},
    application_args=[config_file_path]
)

end = DummyOperator(task_id="end", dag=dag)

start >> [process_airlines, process_airports, process_flights] >> end 