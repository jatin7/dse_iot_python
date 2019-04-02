#!/usr/bin/python

from dse.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from dse.auth import PlainTextAuthProvider
from dse.policies import DCAwareRoundRobinPolicy,TokenAwarePolicy, ConstantSpeculativeExecutionPolicy
from dse import ConsistencyLevel
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from random import randint
from ConfigParser import ConfigParser
import time, json, sys

config = ConfigParser()
config.read('iot.ini')
streamWindow = 5
cbucket = 0
contactpoints= ['127.0.0.1']

cluster = Cluster(contact_points=contactpoints)
session = cluster.connect()

iotwrite = session.prepare("INSERT INTO demo.iot (id, bucket, ts, sensor, type, reading) VALUES (now(), ?, ?, ?, ?, ?)")

def processRow(r):
   bucket = r['bucket']
   ts = r['ts']
   sensor = r['sensor']
   type = r['type']
   reading = r['reading']

   #session.execute(iotwrite, [bucket, ts, sensor, type, reading])
   query = """ INSERT INTO demo.iot (id, bucket, ts, sensor, type, reading) VALUES (now(), '%s', '%s', '%s', '%s', %s) """ % (str(bucket), str(ts), str(sensor), str(type), int(reading))
   query = """ SELECT * FROM demo.iot limit 1 """
   session.execute(query)

   with open('pylog.txt', 'a') as f:
      f.write(query)

   

#Setup Spark Context for DSE
conf = SparkConf().setAppName("Stand Alone Python Script")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#Setup Streaming
ssc = StreamingContext(sc, streamWindow)

kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'dse-iot':1})

parsed = kafkaStream.map(lambda v: json.loads(v[1]))

#parsed.pprint()
parsed.foreachRDD(lambda rdd: rdd.foreach(processRow))
ssc.start()
ssc.awaitTermination()
