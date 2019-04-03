#!/usr/bin/python

from dse.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from dse.auth import PlainTextAuthProvider
from dse.policies import DCAwareRoundRobinPolicy,TokenAwarePolicy, ConstantSpeculativeExecutionPolicy
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from dse import ConsistencyLevel
from kafka import KafkaConsumer
import json

#configs
topic = "dse-iot"
contactpoints= ['127.0.0.1']

#Connect to DSE
cluster = Cluster(contact_points=contactpoints)
session = cluster.connect()

#Setup prepared statements
iotwrite = session.prepare("INSERT INTO demo.iot (id, bucket, ts, sensor, type, reading) VALUES (now(), ?, ?, ?, ?, ?)")

#Setup Spark Context for DSE
conf = SparkConf() \
 .setAppName("IOT Demo Consumer") \
 .set('spark.executor.cores', '3') \
 .set('spark.cores.max', '3') \
 .set('spark.driver.memory','2g')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#connect to kafka
consumer = KafkaConsumer(
    topic,
       bootstrap_servers=['localhost:9092'],
       auto_offset_reset='latest',
       enable_auto_commit=True)

def writeFS(bucket):
   print "Writing out %s to FS" % (bucket)
   table = sqlContext.read.format("org.apache.spark.sql.cassandra").load(table="iot", keyspace="demo")   #.filter(t['bucket'] == bucket)
   rows = table.where(table.bucket==bucket)
   rows.write.mode('append').parquet("demo/iot.pqt/")

lastBucket = "null"
for message in consumer:
    message = json.loads(message.value)
    bucket = str(message['bucket'])
    ts = str(message['ts'])
    sensor = str(message['sensor'])
    stype = str(message['type'])
    reading = int(message['reading'])
    if lastBucket == "null": lastBucket = bucket
    if bucket != lastBucket:
        writeFS(lastBucket)
        lastBucket = bucket
    print(bucket, ts, sensor, stype, reading)
    session.execute(iotwrite, [bucket, ts, sensor, stype, reading])
