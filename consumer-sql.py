#!/usr/bin/python

#from pyspark import SparkContext, SparkConf
#from pyspark.streaming import *
#from pyspark.sql import *
from pyspark.streaming.kafka import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import json


#Setup Spark Context for DSE
#conf = SparkConf().setAppName("Stand Alone Python Script")
#sc = SparkContext(conf=conf)
#sqlContext = SQLContext(sc)
#print sc.version

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

row = ds.select(
   explode(
       spark.parallelize(str(ds.value))
   ).alias("getrow")
)

rdd = sc.parallelize(row)

iotrow = row \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

#print(type(ds))
#ds.printSchema()

