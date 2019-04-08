#!/usr/bin/python
from pyspark.streaming.kafka import *
from pyspark.sql import *
import json

def processRow(df):
    # Transform and write batchDF
    print(type(df))
    pass


print("Building spark session")
spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .getOrCreate()

#Setup Streaming
ds = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "dse-iot") \
  .option("startingOffsets", "latest") \
  .load()

print(type(ds))

dse = ds.writeStream.foreach(processRow) 

dsefs = ds.writeStream \
      .format("parquet") \
      .option("path", "dsefs://localhost:5598/parquet/") \
      .option("checkpointLocation", "dsefs://localhost:5598/checkpoints/") \
      .trigger(processingTime='60 seconds') \
      .outputMode("Append") \
      .start()

dsefs.awaitTermination()

