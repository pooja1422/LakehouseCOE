import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import from_json, col
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
    .csv("s3a://sourcefile-1/Yahoo_Stock_Market.csv")  # Specify your file path

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

kafka_schema = StructType([
    StructField("Date", TimestampType(), True),
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
    .load()

spark.streams.awaitAnyTermination()

kafka_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

processed_df = kafka_df.select("key", "value")

parsed_df = kafka_df.select(from_json(col("value"), kafka_schema).alias("data"))

# Select individual columns from the parsed JSON data
processed_df = parsed_df.select(
    col("data.Date"),
    col("data.Open"),
    col("data.High"),
    col("data.Low"),
    col("data.Close"),
    col("data.Volume"),
    col("data.Dividends"),
    col("data.Stock Splits"),
    col("data.Company") 
)

iceberg_df = spark.read \
    .format("iceberg") \
    .load("s3a://destination.data/iceberg_tables/stockdata").limit(10)

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

s3_output_path = "s3a://streamdata-spark-metadata/"
checkpoint_path = "s3a://destination.data/checkpoint/"  

print("s3_output_path fetched successfully")
kafka_df.printSchema()

query = final_df.writeStream \
    .format("json") \
    .option("checkpointLocation", checkpoint_path) \
    .option("path", s3_output_path) \
    .outputMode("append") \
    .foreachBatch(lambda df, epoch_id: print(f"Processed batch {epoch_id}, number of records: {df.count()}")) \
    .start()

print(spark.streams.active)

# Await termination
spark.streams.awaitAnyTermination()
