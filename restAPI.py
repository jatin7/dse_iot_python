#!/usr/bin/python

from flask import Flask, jsonify, abort, request, make_response, url_for, Response
from flask_cors import CORS, cross_origin
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from ssl import PROTOCOL_TLSv1, CERT_REQUIRED, CERT_OPTIONAL

from dse.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from dse.auth import PlainTextAuthProvider
from dse.query import dict_factory
from dse.policies import DCAwareRoundRobinPolicy,TokenAwarePolicy, ConstantSpeculativeExecutionPolicy
from dse import ConsistencyLevel

import json

#Config Section
contactpoints= ['127.0.0.1']



#End Config

#Setup Flask API service
app = Flask(__name__)
CORS(app)

@app.before_first_request
def setupSpark():
   print "Setting up spark connection"
   global sqlContext
   conf = SparkConf().setAppName("RestAPI")#.set("spark.driver.allowMultipleContexts", "true")
   sc = SparkContext(conf=conf)
   sqlContext = SQLContext(sc)
   parquetFile = sqlContext.read.parquet("demo/iot.pqt/")
   parquetFile.createOrReplaceTempView("dseiot")

   print "Setting up DSE connection"
   global session
   cluster = Cluster(contact_points=contactpoints)
   session = cluster.connect()
   session.row_factory = dict_factory

#refreshSQL()

@app.route('/batch/read', methods=['POST'])
def batchRead():
   keys = {'bucket': None, 'sensor': None, 'reading': None, 'type': None, 'ts': None}
   if not request.json:
      abort(400)
   for k in keys:
      if k in request.json: 
         keys[k] = request.json[k] 
   query = "SELECT * FROM demo.iot WHERE"
   for k in keys:
      if keys[k] != None:
         query = query + " AND " + k + " = " + keys[k]
   query = query.replace('WHERE AND', 'WHERE')
   rows = sqlContext.sql(query)
   d = map(lambda row: row.asDict(), rows.collect())
   return json.dumps(d)


@app.route('/rt/read', methods=['POST'])
def rtRead():
   if not request.json or not 'bucket' in request.json or not 'sensor' in request.json:
      abort(400)
   query = """SELECT * FROM demo.iot WHERE bucket = '%s' and sensor = '%s'""" % (str(request.json['bucket']), str(request.json['sensor']))
   rows = session.execute(query)
   v = []
   for r in rows:
      v = v + list(r)
   return json.dumps(v)


#rows = sqlContext.sql("SELECT * FROM dseiot where reading = 25")
#d = map(lambda row: row.asDict(), rows.collect())
#print(json.dumps(d))

app.run(debug=True,port=8080)

