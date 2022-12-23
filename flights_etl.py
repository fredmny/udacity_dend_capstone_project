import logging
import configparser
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
import os

logging.basicConfig(level=logging.INFO)

config = configparser.ConfigParser()
config.read('dl.cfg')

aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID']
aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
  spark = SparkSession \
    .builder \
    .config(
      "spark.jars.packages", 
      "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ) \
    .config(
      'spark.hadoop.fs.s3a.aws.credentials.provider', 
      'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'
    )\
    .config(
      "spark.hadoop.fs.s3a.access.key", 
      aws_access_key_id
    ) \
    .config(
      "spark.hadoop.fs.s3a.secret.key", 
      aws_secret_access_key
    ) \
    .getOrCreate()
  
  return spark

def process_airline_data(spark, input_path, output_path):
  airlines_path = 'airlines/airlines.csv'
  dim_table = 'dim_airlines.parquet'

  # Reading raw data from S3
  logging.info('Creating stg_airlines spark dataframe')
  stg_airlines = spark.read.csv(
    os.path.join(input_path, airlines_path), 
    header=True
  )

  logging.info(f'Transforming data from {airlines_path}')

  # Selecting columns from stg df
  values_to_exclude = ['&T', '++', '-+', '--', '..', '-']
  dim_airlines = stg_airlines.selectExpr(
    '`Airline ID` as id', # will be used to deduplicate the iata codes
    'IATA as iata_code',
    'ICAO as icao_code',
    'Name as name',
    'Callsign as callsign',
    'Country as country',
    'Active = "Y" as active'
  ).filter(
    (F.col('iata_code').isNotNull()) & \
    (~F.col('iata_code').isin(values_to_exclude)) # '~' negates the condition
  )

  # Window function to late filter the results based on condition
  w = Window \
    .partitionBy('iata_code') \
    . orderBy([F.desc('active'), F.asc('id')])

  # Apply window function
  dim_airlines = dim_airlines.withColumn(
    'row_n', 
    F.row_number().over(w)
  )

  # Filter rows based on applied order
  dim_airlines = dim_airlines.filter(F.col('row_n') == 1)
  dim_airlines = dim_airlines.drop('row_n', 'id')

  #Writh data to s3 bucket
  logging.info(f'Writing data into {os.path.join(output_path, dim_table)}')
  dim_airlines.write.partitionBy('country').mode('overwrite')\
    .parquet(os.path.join(output_path, dim_table))
    
def process_airport_data(spark, input_path, output_path):
  airports_path = 'airports/airport-codes_json.json'
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
  
def process_flights_data(spark, input_path, output_path):
  flights_path = 'flights/*'
  dim_table = 'dim_date.parquet'
  fct_table = 'fct_flights.parquet'
  
  # Reading raw data from S3
  logging.info('Creating stg_flights spark dataframe')
  stg_flights = spark.read.csv(
    os.path.join(input_path, flights_path), 
    header=True
  )

  # --- Data processing for `dim_dates` creation --- #
  
  logging.info(f'Transforming data from {flights_path} to create {dim_table}')
  # Selecting column from stg df# Selecting columns from stg df
  dim_dates = stg_flights.selectExpr('DATE(FL_DATE) AS dt').drop_duplicates()
  
  # Creating other columns from main column
  dim_dates = dim_dates\
    .withColumn('year', F.year('dt'))\
    .withColumn('month', F.month('dt'))\
    .withColumn('day', F.dayofmonth('dt'))\
    .withColumn('quarter', F.quarter('dt'))\
    .withColumn('semester', F.when(F.col('quarter').isin(1,2), 1).otherwise(2))

  # Write to s3 bucket in parquet format. This is a small table and no 
  # partitioning is needed
  logging.info(f'Writing data into {os.path.join(output_path, dim_table)}')
  dim_dates.write.mode('overwrite')\
    .parquet(os.path.join(output_path, dim_table))

  # --- Data processing for `fct_flights` creation --- #

  logging.info(f'Transforming data from {flights_path} to create {fct_table}')
  # select column with basic conditions
  fct_flights = stg_flights.selectExpr(
    'DATE(FL_DATE) as dt',
    'OP_CARRIER as airline_iata_code',
    'INT(OP_CARRIER_FL_NUM) as flight_number',
    'ORIGIN as origin_airport',
    'DEST as destination_airport',
    'INT(CRS_DEP_TIME) as crs_departure_time',
    'INT(DEP_TIME) as departure_time',
    'INT(CRS_ARR_TIME) as crs_arrival_time',
    'INT(ARR_TIME) as arrival_time',
    'INT(WHEELS_OFF) as wheels_off_time',
    'INT(WHEELS_ON) as wheels_on_time',
    'INT(AIR_TIME) as air_time',
    'INT(CRS_ELAPSED_TIME) as crs_elapsed_time',
    'INT(ACTUAL_ELAPSED_TIME) as actual_elapsed_time',
    'INT(DEP_DELAY) as delay_time',
    'COALESCE(INT(DEP_DELAY) > 0, FALSE) as is_delayed',
    'COALESCE(INT(CARRIER_DELAY) > 0, FALSE) as delay_is_carrier',
    'CANCELLED = 1 as is_cancelled',
    'CANCELLATION_CODE as cancellation_code',
    'CASE \
      WHEN GREATEST(\
      INT(CARRIER_DELAY), INT(WEATHER_DELAY), INT(NAS_DELAY), INT(SECURITY_DELAY)\
      ) = 0 THEN NULL \
      WHEN GREATEST(\
      INT(CARRIER_DELAY), INT(WEATHER_DELAY), INT(NAS_DELAY), INT(SECURITY_DELAY)\
      ) = INT(CARRIER_DELAY) THEN "carrier"\
      WHEN GREATEST(\
      INT(CARRIER_DELAY), INT(WEATHER_DELAY), INT(NAS_DELAY), INT(SECURITY_DELAY)\
      ) = INT(WEATHER_DELAY) THEN "weather"\
      WHEN GREATEST(\
      INT(CARRIER_DELAY), INT(WEATHER_DELAY), INT(NAS_DELAY), INT(SECURITY_DELAY)\
      ) = INT(NAS_DELAY) THEN "nas"\
      WHEN GREATEST(\
      INT(CARRIER_DELAY), INT(WEATHER_DELAY), INT(NAS_DELAY), INT(SECURITY_DELAY)\
      ) = INT(SECURITY_DELAY) THEN "security"\
      ELSE NULL\
    END as main_delay_reason'
  )

  # Create udf to adjust format of columns with times from format
  # 'hhmm.0' to 'hh:mm'
  def convert_time(value):
    value = f'{int(value):04}'
    value = f'{value[:2]}:{value[2:]}'
    return value

  convertTime = F.udf(lambda value: convert_time(value))

  # format time of day columns
  fct_flights = fct_flights \
    .withColumn('crs_departure_time', convertTime(F.col('crs_departure_time')))\
    .withColumn('departure_time', convertTime(F.col('departure_time')))\
    .withColumn('crs_arrival_time', convertTime(F.col('crs_arrival_time')))\
    .withColumn('arrival_time', convertTime(F.col('arrival_time')))\
    .withColumn('wheels_off_time', convertTime(F.col('wheels_off_time')))\
    .withColumn('wheels_on_time', convertTime(F.col('wheels_on_time')))

  # Create primary key for table
  fct_flights = fct_flights.withColumn(
    'pk', 
    F.concat(
      F.coalesce(F.col('dt'), F.lit('-')),
      F.coalesce(F.col('crs_departure_time'), F.lit('-')),
      F.coalesce(F.col('airline_iata_code'), F.lit('-')),
      F.coalesce(F.col('flight_number'), F.lit('-'))
    )
  )
  
  fct_flights.select('pk', 'dt', 'crs_departure_time').show()
  
  # Testing the primary key of the fact table to see if it has
  # duplicates or null values
  logging.info(f'Checking fct_flights\' pk for duplicates and null values')
  df_check = fct_flights.select('pk')
  if df_check.count() > df_check.drop_duplicates().count():
    raise ValueError('Primary key of fct_flights has duplicates')
  df_null = df_check.filter(df_check.pk.isNull())
  if df_null.count() > 0:
    raise ValueError('Primary key of fct_flights has null values')
  
  #Writh data to s3 bucket
  logging.info(f'Writing data into {os.path.join(output_path, fct_table)}')
  fct_flights.write.partitionBy('dt').mode('append')\
    .parquet(os.path.join(output_path, fct_table))
  
def main():
  input_path='s3a://fw-flights-source'
  output_path='s3a://fw-flights-tbl'
  spark = create_spark_session()
  process_airline_data(spark, input_path, output_path)
  process_airport_data(spark, input_path, output_path)
  process_flights_data(spark, input_path, output_path)

if __name__ == '__main__':
  main()