#!/usr/bin/python
#
# Project: Analyze temperature and windspeed sensor data sent hourly
#
# Student: Sheetal Gangakhedkar
#
# Description: This is a temperature and windspeed sensor device installed 
# outside, and it is a IoT device, so it can transmit the current readings
# hourly to our BigData analysis engine in the Cloud.
#
# Objective: 
# Perform Kafka Streaming Analysis using Spark, to compute - 
#    * Average, Min, and Max daily temperatures.
#    * Average, Min, and Max monthly temperatures.
#    * Average, Min, and Max yearly temperatures.
#    * Average, Min, and Max daily windspeeds.
#    * Average, Min, and Max monthly windspeeds.
#    * Average, Min, and Max yearly windspeeds.
#
# Usage:
# ./iotsimulator.py 24 - generates hourly temperature & windspeed data for last 24 hrs (1 sensor).
# ./iotsimulator.py 10000 - generates hourly temperature & windspeed data for last 10000 hrs (1 sensor).
#
# Implementation uses Python 2.7
#
import sys
import datetime
import random
import string
import time
import os

# Set number of simulated messages to generate
if len(sys.argv) > 1:
  numMsgs = int(sys.argv[1])
else:
  numMsgs = 1

# Fixed values for San Jose, CA area home - change for other regions
guidStr = "0-ZZZ12345678-01A"
destinationStr = "0-AAA12345678"
locationStr = "37.312125, -121.757265"
formatStr = "urn:example:sensor:tempwind"
os.environ['TZ'] = 'US/Pacific'
time.tzset()
tzoffsec = time.timezone
#tzoffsec = 28800
nowTime = datetime.datetime.utcnow()
startTime = nowTime - datetime.timedelta(hours=numMsgs)
# average monthly high and low temperatures for this location
# Reference: https://www.usclimatedata.com/climate/san-jose/california/united-states/usca0993
avgHighLowF = [ [60,42], [64,45], [67,47],[70,49],[75,53],[81,56],[84,58],[84,58],[82,57],[76,53],[67,46],[61,42] ]
# average monthly windspeeds for this location
# Reference: https://wind.willyweather.com/ca/santa-clara-county/san-jose.html
avgHighLowW = [ [0,9.5], [0,12.8], [0,14.5],[0,15.6],[0,16.4],[0,15.4],[0,15.1],[0,15.0],[0,15.2],[0,13.2],[0,11.1],[0,11.7] ]
# max temperatures usually reach around local afternoons, hourly fractions
fracByHour = [ 0.1, 0.1, 0.1, 0.1, 0.1, 0.2, 0.2, 0.3, 0.4, 0.5, 0.6, 0.8, 0.9, 1.0, 1.0, 0.9, 0.9, 0.8, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2 ]

# Choice for random letter
letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

iotmsg_header = """\
{
  "guid": "%s",
  "destination": "%s",
  "location": "%s", """

iotmsg_eventTime = """\
  "eventTime": "%sZ", """

iotmsg_payload ="""\
  "payload": {
     "format": "%s", """

iotmsg_data ="""\
     "data": {
       "temperature": %.1f,
       "windspeed": %.1f
     }
   }
}"""

##### Generate JSON output:

print("[")

dataElementDelimiter = ","
for counter in range(0, numMsgs):

  print(iotmsg_header % (guidStr, destinationStr, locationStr))

  msgTime = startTime + datetime.timedelta(hours=counter)
  localMsgTime = msgTime - datetime.timedelta(seconds=tzoffsec)
  print(iotmsg_eventTime % (msgTime.isoformat()))

  print(iotmsg_payload % (formatStr))

  # check month for average min and max tables
  monIdx = msgTime.month - 1
  dayhour = localMsgTime.hour
  maxAvgTemp = avgHighLowF[monIdx][1] + (avgHighLowF[monIdx][0] - avgHighLowF[monIdx][1]) * fracByHour[dayhour]
  
  # Generate a random floating point number, for a given range
  randTemp = random.uniform(avgHighLowF[monIdx][1], maxAvgTemp) + random.uniform(-2, 2)
  randSpeed = random.uniform(avgHighLowW[monIdx][1], avgHighLowW[monIdx][0])
  
  if counter == numMsgs - 1:
    dataElementDelimiter = ""
  print(iotmsg_data % (randTemp, randSpeed) + dataElementDelimiter)

print("]")
