import logging
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pyspark.sql import SparkSession

class processDataSpark(BaseOperator):
  
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
      .config(
        "spark.jars.packages", 
        "org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.888"
      ) \
      .config(
        'spark.hadoop.fs.s3a.aws.credentials.provider', 
        'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'
      )\
      .config(
        "spark.hadoop.fs.s3a.access.key", 
        aws_access_key
      ) \
      .config(
        "spark.hadoop.fs.s3a.secret.key", 
        aws_secret_key
      ) \
      .getOrCreate()
   
    self.spark_function(spark, self.input_path, self.output_path)