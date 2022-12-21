import os
from pyspark.sql import Window
from pyspark.sql import functions as F
class SparkFunctions:
  def process_airlines_data(spark, input_path, output_path):
    airlines_path = 'airlines/airlines.csv'
    dim_table = 'dim_airlines'
    
    stg_airlines = spark.read.csv(
      os.path.join(input_path, airlines_path), 
      header=True
    )

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
      (~F.col('iata_code').isin(values_to_exclude)) # The '~' negates the condition
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

    dim_airlines.write.partitionBy('country').mode('overwrite')\
    .parquet(os.path.join(output_path, dim_table))
      
  def process_airports_data(spark, input_path, output_path):
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