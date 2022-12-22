import logging
# from airflow.contrib.hooks.aws_hook import AwsHook
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
import os

input_path = 's3a://fw-flights-source'
output_path = 's3a://fw-flights-tables'
flights_path = 'flights/2018.csv'
stg_flights = spark.read.csv(
  os.path.join(input_path, flights_path), 
  header=True)

# Data processing for `dim_dates` creation
dim_dates = stg_flights.selectExpr('DATE(FL_DATE) AS dt').drop_duplicates()
dim_dates = dim_dates\
  .withColumn('year', F.year('dt'))\
  .withColumn('month', F.month('dt'))\
  .withColumn('day', F.dayofmonth('dt'))\
  .withColumn('quarter', F.quarter('dt'))\
  .withColumn('semester', F.when(F.col('quarter').isin(1,2), 1).otherwise(2))

dim_dates.write.mode('overwrite')\
.parquet(os.path.join(output_path, dim_table))

# Data processing for `fct_flights` creation