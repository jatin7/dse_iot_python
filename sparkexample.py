from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

#Setup Spark Context for DSE
conf = SparkConf().setAppName("Stand Alone Python Script")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#load table from DSE
table = sqlContext.read.format("org.apache.spark.sql.cassandra").load(table="table1", keyspace="demo")

#Filter data 
rows = table.where(table.bucket==20193261840).where(table.data1==61)
rows.show()

#Write data to DSEFS
rows.write.mode('append').parquet("demo2/table1.pqt/")

#Load data from DSEFS
parquetFile = sqlContext.read.parquet("demo2/table1.pqt/")
parquetFile.createOrReplaceTempView("parquetFile")
rows = sqlContext.sql("SELECT * FROM parquetFile where data2 = 25")
rows.show()
