# LakehouseCOE
# Directions

To start up the different containers individually.

spark 3.3 with jupyter notebook

```
docker-compose up <required-services>
```
Use Cases
This project contains three distinct use cases:

PySpark Code:
A script that transfers data from HDFS to S3 using PySpark.

Kafka Producer:
A Kafka producer script to stream data.

Iceberg Table Creation:
A folder containing SQL queries to create and interact with Iceberg tables.
