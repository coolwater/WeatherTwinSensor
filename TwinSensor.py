# Databricks notebook source
# /FileStore/tables/twinsensor_iotdata.js
# Header imports
from __future__ import print_function
from operator import add
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import window

import sys
import json
import re

conf = SparkConf()
#conf.set("spark.ui.showConsoleProgress", "false")
# sc = SparkContext(appName="PythonSparkScriptExample", conf=conf)

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
from pyspark.sql.functions import mean, min, max

#----------- Your Code Below -----------

# Inject JSON to a JSON RDD
jsonRDD = sc.wholeTextFiles("/FileStore/tables/twinsensor_iotdata.js").map(lambda (k,v): v)
js = jsonRDD.map(lambda x: re.sub(r"\s+", "", x, flags=re.UNICODE))

iotmsgsRDD = sqlContext.read.json(js)
iotmsgsRDD.registerTempTable("iotmsgsTable")

weeklyIoTGDF = \
  iotmsgsRDD \
    .groupBy("guid", window("eventTime", "1 week", "1 week")) \
    .agg(min('payload.data.temperature'), max('payload.data.temperature'), mean('payload.data.temperature'), min('payload.data.windspeed'), max('payload.data.windspeed'), mean('payload.data.windspeed')) \
    .orderBy("window.start")
    
print("Show Weekly IoT Sensor Stats")
weeklyIoTGDF.show()

dailyIoTGDF = \
  iotmsgsRDD \
    .groupBy("guid", window("eventTime", "1 days", "1 days")) \
    .agg(min('payload.data.temperature'), max('payload.data.temperature'), mean('payload.data.temperature'), \
         min('payload.data.windspeed'), max('payload.data.windspeed'), mean('payload.data.windspeed')) \
    .orderBy("window.start")

print("Show Daily IoT Sensor Stats")
dailyIoTGDF.show()


# print("Show IoT Sensor temperatures sorted in descending order")
# sqlContext.sql("select cast(payload.data.temperature as float) \
#  from iotmsgsTable order by temperature desc").show()

# print("Show IoT Sensor windspeed sorted in descending order")
# sqlContext.sql("select cast(payload.data.windspeed as float) \
#  from iotmsgsTable order by windspeed desc").show()

print("Show SUMMARY of IoT Sensor temperature data")
sqlContext.sql("select cast(payload.data.temperature as float) \
  from iotmsgsTable order by temperature desc").describe().show()

print("Show SUMMARY of IoT Sensor windspeed data")
sqlContext.sql("select cast(payload.data.windspeed as float) \
  from iotmsgsTable order by windspeed desc").describe().show()


# COMMAND ----------


