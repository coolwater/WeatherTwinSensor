#

"""
 Processes direct stream from kafka, '\n' delimited text directly received 
   every 2 seconds.
 Usage: kafka-direct-iotmsg.py <broker_list> <topic>

 To run this on your local machine, you need to setup Kafka and create a 
   producer first, see:
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      kafka-direct-iotmsg.py \
      localhost:9092 iotmsgs`
"""
from __future__ import print_function

import sys
import re
import json

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import OffsetRange
from pyspark.sql.functions import window
from pyspark.sql.functions import mean, min, max
from operator import add

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka-direct-iotmsg.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)
    sqlContext = SQLContext(sc)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    # Read in the Kafka Direct Stream into a TransformedDStream
    jsonDStream = kvs.map(lambda (k,v): v) 
	
    ########################
    # 
    # Processing Function
    #
    ########################
    def process(time, rdd):
        print("TIME OF IoT MESSAGES RECEPTION: %s" % str(time))

        try:
            # Parse the one line JSON string from the DStream RDD
            jsonString = rdd.map(lambda x: \
                     re.sub(r"\s+", "", x, flags=re.UNICODE)).reduce(add)
            print("\nJSON String in RDD = \n%s\n" % str(jsonString))

            # Convert the JSON string to an RDD
            jsonRDDString = sc.parallelize([str(jsonString)])

            # Convert the JSON RDD to Spark SQL Context
            jsonRDD = sqlContext.read.json(jsonRDDString)

            # Register the JSON SQL Context as a temporary SQL Table
            jsonRDD.registerTempTable("iotmsgsTable")

            #################################################
            # Processing of RDD with IoT sendor data goes here
            #################################################

			weeklyIoTGDF = \
			  jsonRDD \
				.groupBy("guid", window("eventTime", "1 week", "1 week")) \
				.agg(min('payload.data.temperature'), max('payload.data.temperature'), mean('payload.data.temperature'), min('payload.data.windspeed'), max('payload.data.windspeed'), mean('payload.data.windspeed')) \
				.orderBy("window.start")
				
			print("Show Weekly IoT Sensor Stats")
			weeklyIoTGDF.show()

			dailyIoTGDF = \
			  jsonRDD \
				.groupBy("guid", window("eventTime", "1 days", "1 days")) \
				.agg(min('payload.data.temperature'), max('payload.data.temperature'), mean('payload.data.temperature'), \
					 min('payload.data.windspeed'), max('payload.data.windspeed'), mean('payload.data.windspeed')) \
				.orderBy("window.start")

			print("Show Daily IoT Sensor Stats")
			dailyIoTGDF.show()

            # print("Show IoT Sensor temperatures sorted in descending order")
            # sqlContext.sql("select payload.data.temperature \
            # from iotmsgsTable order by temperature desc").show()

            # print("Show IoT Sensor windspeed sorted in descending order")
            # sqlContext.sql("select payload.data.windspeed \
            # from iotmsgsTable order by windspeed desc").show()

            print("Show SUMMARY of IoT Sensor temperature data")
            sqlContext.sql("select payload.data.temperature \
            from iotmsgsTable order by temperature desc").describe().show()

            print("Show SUMMARY of IoT Sensor windspeed data")
            sqlContext.sql("select payload.data.windspeed \
            from iotmsgsTable order by windspeed desc").describe().show()

            # Clean-up
            sqlContext.dropTempTable("iotmsgsTable")

        # Catch any exceptions
        except:
            pass

    # Process each RDD of the DStream coming in from Kafka
    jsonDStream.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()

