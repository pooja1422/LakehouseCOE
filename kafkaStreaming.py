import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import split, col, to_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

conf = (
    pyspark.SparkConf()
    .setAppName('Streaming_usecase')
    .set('spark.jars.packages',
         'software.amazon.awssdk:bundle:2.17.178,'
         'software.amazon.awssdk:url-connection-client:2.17.178,'
         'org.apache.hadoop:hadoop-aws:3.3.1,'
         'org.apache.iceberg:iceberg-spark3-runtime:0.13.2,'
         'org.apache.iceberg:iceberg-core:0.13.2,'
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1') 
    .set('spark.hadoop.fs.s3a.access.key', S3_ACCESS_KEY)
    .set('spark.hadoop.fs.s3a.secret.key', S3_SECRET_KEY)
    .set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")  # S3 endpoint
    .set('spark.hadoop.fs.defaultFS', 'hdfs://namenode:9000')  # HDFS Configuration
    .set("spark.hadoop.fs.s3a.connection.maximum", "100")
    .set("spark.hadoop.fs.s3a.path.style.access", "true")
    .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")  # Iceberg Catalog configuration
    .set("spark.sql.catalog.spark_catalog.type", "hadoop")  # Catalog type
    .set("spark.sql.catalog.spark_catalog.warehouse", "s3a://destination.data/")  # Iceberg warehouse
    .set("spark.sql.streaming.checkpointLocation", "s3://destination.data/checkpoint/")

)



# Kafka configuration
kafka_servers = "kafka:29092"  
kafka_topic = "lakehouse_topic"  

# Initialize Spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()

print("*** Spark session creation  ***")

# Define the schema for your data (as per the columns you provided)
schema = StructType([
    StructField("Symbol", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Change", DoubleType(), True),
    StructField("Market Cap", DoubleType(), True),
    StructField("PE_ratio", DoubleType(), True)
])

# Step 1: Read Data from S3 (sourcefile-1 bucket)
s3_df = spark.read \
    .schema(schema) \
    .csv("s3a://sourcefile-1/Yahoo_Stock_Market.csv")  

print("*** s3 data fetched ***")

spark.conf.set("spark.sql.catalog.mycatalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.mycatalog.type", "hadoop")
spark.conf.set("spark.sql.catalog.mycatalog.warehouse", "s3a://destination.data/iceberg_tables/")


spark.sql("""
    CREATE TABLE IF NOT EXISTS mycatalog.stockdata (
        Symbol STRING,
        Name STRING,
        Price DOUBLE,
        Change DOUBLE,
        `Market Cap` DOUBLE,
        PE_ratio DOUBLE
    )
    USING iceberg
""")

#Write the data to the Iceberg table
s3_df.write \
    .format("iceberg") \
    .mode("overwrite") \
    .save("mycatalog.stockdata") 

print("*** Data to the Iceberg table ***")

# Define the schema for your CSV data
csv_schema = StructType([
    StructField("Date", StringType(), True),  
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", DoubleType(), True),
    StructField("Dividends", DoubleType(), True),
    StructField("Stock Splits", DoubleType(), True),
    StructField("Company", StringType(), True)
])

# Step 3: Read Kafka stream
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", kafka_topic) \
    .option("failOnDataLoss", "false") \
    .load()

kafka_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Parse CSV data
parsed_df = kafka_df.select(split(col("value"), ",").alias("csv_data"))

# Select individual columns and cast to correct types
processed_df = parsed_df.select(
    to_timestamp(col("csv_data")[0], "yyyy-MM-dd HH:mm:ssXXX").alias("Date"), #Parse the date column
    col("csv_data")[1].cast("double").alias("Open"),
    col("csv_data")[2].cast("double").alias("High"),
    col("csv_data")[3].cast("double").alias("Low"),
    col("csv_data")[4].cast("double").alias("Close"),
    col("csv_data")[5].cast("double").alias("Volume"),
    col("csv_data")[6].cast("double").alias("Dividends"),
    col("csv_data")[7].cast("double").alias("Stock Splits"),
    col("csv_data")[8].alias("Company")
).filter(col("Date").isNotNull())
 
parsed_df.printSchema()
processed_df.printSchema()

iceberg_df = spark.read \
    .format("iceberg") \
    .load("s3a://destination.data/iceberg_tables/stockdata")

print("*** Data fetched from iceberg table ***")

# Perform the join
final_df = processed_df.join(
    iceberg_df, processed_df['Company'] == iceberg_df['Symbol'], 'left'
) \
    .select(
        processed_df['Date'], 
        processed_df['Company'], 
        iceberg_df['Name'], 
        iceberg_df['Price'], 
        iceberg_df['Change']
    )

print("*** Joining completed ***")

s3_output_path = "s3a://sourcefile-1/processeddata/"
checkpoint_path = "s3a://destination.data/checkpoint/"  

print("s3_output_path fetched successfully")
kafka_df.printSchema()

query = final_df.writeStream \
    .format("json") \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .foreachBatch(lambda df, epoch_id: df.write.json(f"s3a://sourcefile-1/processeddata/batch-{epoch_id}")) \
    .start()


print(spark.streams.active)

# Await termination
spark.streams.awaitAnyTermination()
