import logging
# from airflow.contrib.hooks.aws_hook import AwsHook
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
import os

input_path='s3a://fw-flights-source'
output_path='s3a://fw-flights-tables'
airports_path = 'airports/airports-codes_json.json'
dim_table = 'dim_airports'

stg_airports = spark.read.json(
  os.path.join(input_path, airports_path)
)

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

dim_airports.write.partitionBy('iso_country').mode('overwrite')\
.parquet(os.path.join(output_path, dim_table))