#!/usr/bin/python
from kafka import KafkaProducer
from random import randint
from ConfigParser import ConfigParser
import time

config = ConfigParser()
config.read('iot.ini')

producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')

sensorCount = config.get('IOT','devices')
sensorTypes = config.get('IOT','types').split(',')

while 1:
   current = time.localtime()
   bucket = str(current.tm_year) + str(current.tm_mon) + str(current.tm_mday) + str(current.tm_hour) + str(current.tm_min)
   ts = time.strftime('%Y-%m-%dT%H:%M:%S', current)
   sensor = randint(1,int(sensorCount))
   for t in sensorTypes:
      reading = randint(1,100)
      msg = bucket + "," + str(ts) + "," + str(sensor) + "," + t + "," + str(reading)
      print msg
      #producer.send("dse-iot", msg.strip())
      #producer.flush()
