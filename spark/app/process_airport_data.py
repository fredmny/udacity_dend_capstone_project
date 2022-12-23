import logging
# from airflow.contrib.hooks.aws_hook import AwsHook
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
import os

spark = SparkSession \
  .builder \
  .config(
    "spark.jars.packages", 
    "org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375"
  ) \
  .config(
    'spark.hadoop.fs.s3a.aws.credentials.provider', 
    'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'
  )\
  .config(
    "spark.hadoop.fs.s3a.access.key", 
    ''
  ) \
  .config(
    "spark.hadoop.fs.s3a.secret.key", 
    ''
  ) \
  .getOrCreate()

input_path='s3a://fw-flights-source'
output_path='s3a://fw-flights-tbl'
airports_path = 'airports/airports-codes_json.json'
dim_table = 'dim_airports.parquet'

# Reading raw data from S3
logging.info('Creating stg_airports spark dataframe')
stg_airports = spark.read.json(
  os.path.join(input_path, airports_path)
)

logging.info(f'Transforming data from {airports_path}')

# Selecting columns from stg df
dim_airports = stg_airports.selectExpr(
  'iata_code',
  'name',
  'INT(elevation_ft) as elevation',
  'gps_code',
  'iso_country',
  'iso_region',
  'municipality',
  'type',
  'FLOAT(SPLIT(coordinates, ",")[0]) as longitude',
  'FLOAT(SPLIT(coordinates, ",")[1]) as latitude'
).filter(
  F.col('iata_code').isNotNull() & \
  ~F.col('iata_code').isin('-', '0')
)

# Creating a map and then apply it to the type column 
# to later filter out duplicates
airport_mapping = {
  'large_airport': '1',
  'seaplane_base': '5',
  'heliport': '4',
  'closed': '6',
  'medium_airport': '2',
  'small_airport': '3'
}

dim_airports = dim_airports.withColumn(
  'airport_code',
  F.col('type')
).replace(
  to_replace=airport_mapping,
  subset=['airport_code']
)

dim_airports = dim_airports.withColumn(
  'airport_code',
  dim_airports['airport_code'].cast('integer')
)

# Window function to late filter the results based on condition
w = Window \
  .partitionBy('iata_code') \
  . orderBy('airport_code')

# Apply window function
dim_airports = dim_airports.withColumn(
  'row_n',
  F.row_number().over(w)
)

# Filter rows based on applied order
dim_airports = dim_airports.filter(F.col('row_n') == 1)
dim_airports = dim_airports.drop('row_n', 'airport_code')

logging.info(f'Writing data into {os.path.join(output_path, dim_table)}')
dim_airports.write.partitionBy('iso_country').mode('overwrite')\
  .parquet(os.path.join(output_path, dim_table))