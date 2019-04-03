#!/usr/bin/python
from kafka import KafkaProducer
from random import randint
from ConfigParser import ConfigParser
import time
import json

config = ConfigParser()
config.read('iot.ini')

producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')

sensorCount = config.get('IOT','devices')
sensorTypes = config.get('IOT','types').split(',')

print("Producing!")
while 1:
   #time.sleep(0.5)
   current = time.localtime()
   bucket = str(current.tm_year) + str(current.tm_mon) + str(current.tm_mday) + str(current.tm_hour) + str(current.tm_min)
   ts = time.strftime('%Y-%m-%dT%H:%M:%S', current)
   sensor = randint(1,int(sensorCount))
   for t in sensorTypes:
      reading = randint(1,100)
      data = {}
      data['bucket'] = bucket
      data['ts'] = ts 
      data['sensor'] = sensor 
      data['reading'] = reading
      data['type'] = t
      #print json.dumps(data)
      producer.send("dse-iot", json.dumps(data))
      producer.flush()
