import pyspark
from pyspark.sql import SparkSession

import time

S3_ACCESS_KEY = "AKIAYQYUBGKSFHBXZA4B"
S3_SECRET_KEY = "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"

conf = (
    pyspark.SparkConf()
    .setAppName('HDFS_to_S3')
    .set('spark.jars.packages',
         'software.amazon.awssdk:bundle:2.17.178,'
         'software.amazon.awssdk:url-connection-client:2.17.178,'
         'org.apache.hadoop:hadoop-aws:3.3.1')
    # Set S3 credentials for Hadoop S3 connector (s3a)
    .set('spark.hadoop.fs.s3a.access.key', S3_ACCESS_KEY)
    .set('spark.hadoop.fs.s3a.secret.key', S3_SECRET_KEY)
    .set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")  # S3 endpoint
    .set('spark.hadoop.fs.defaultFS', 'hdfs://namenode:9000')  # HDFS Configuration
    .set("spark.hadoop.fs.s3a.connection.maximum", "100")
    .set("spark.hadoop.fs.s3a.path.style.access", "true")
)


hdfs_path = "hdfs://namenode:9000/stock_details_5_years.csv"

spark = SparkSession.builder.config(conf=conf).getOrCreate()
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

print("Data successfully fetched from HDFS!")

timestamp = int(time.time())
s3_bucket_path = f"s3a://sourcefile-1/stock_details_{timestamp}/"

df.write.csv(s3_bucket_path, header=True)

print("Data successfully copied from HDFS to S3!")