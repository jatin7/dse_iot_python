# dse-iot-python
### USE CASE: Ingest IOT sensor data, provide REST API to read data. Newer (hot) data requires low latency read SLAs while older (cold) data has to be available but can have higher SLAs.

High level solution:
* Write sensor data to kafka
* Stream sensor data into DSE with TTL
* Rollup data from DSE into Parquet files stored on DSE
* Python+Flask based API. CQL for hot reads (DSE), SparkSQL for cold reads (DSEFS)

Requirements:
* DSE 6+ with Analytics enabled
* Kafka (Setup below)
* Python Flask
* PySpark

### Notes:

* Clone repo:

`git clone https://github.com/russkatz/dse-iot-python/ && cd dse-iot-python`


* Setup Kafka:

`./startup all`


* Start producer:
`./producter.py`

* Start consumer:
`dse spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.2.2 consumer.py`

* Start REST API:
`dse spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.2.2 restAPI.py`

* Messing with structured streaming:
`dse spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.2 consumer-sql.py`

* Curl examples:
`curl -s --header "Content-Type: application/json" --request POST --data '{"bucket": "2019431954", "sensor": "843", "type": "temp"}' http://localhost:8080/batch/read`

`curl -s --header "Content-Type: application/json" --request POST --data '{"bucket": "2019431954", "sensor": "843", "type": "temp"}' http://localhost:8080/rt/read`
