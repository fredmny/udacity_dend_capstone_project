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