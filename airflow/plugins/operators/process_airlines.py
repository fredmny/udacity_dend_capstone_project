from airflow.operators import BaseOpeator
from airflow.contrib.hooks import AwsHook
from airflow.utils.decorators import apply_defaults
from pyspark.sql import SparkSession

class processAirlinesOperator(BaseOpeator):
  
  def __init__(
    self,
    input_path='',
    output_path='',
    spark_function='',
    aws_credentials_id='aws_credentials',
    *args, **kwargs
  ):
    super().__init__(*args, **kwargs)
    self.input_path = input_path
    self.output_path = output_path
    self.spark_function = spark_function
    self.aws_credentials_id = aws_credentials_id
  def execute(self, context):
    
    aws_hook = AwsHook(self.aws_credentials_id)
    credentials = aws_hook.get_credentials()
    aws_access_key = credentials.access_key
    aws_secret_key = credentials.secret_key
    
    spark = SparkSession \
      .builder \
      .getOrCreate()
    spark.conf.set("spark.hadoop.fs.s3a.access.key", aws_access_key)
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
   
    self.spark_function(spark, self.input_path, self.output_path)