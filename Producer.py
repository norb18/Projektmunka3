# Databricks notebook source
!pip install confluent_kafka
!pip install kafka-python

# COMMAND ----------

from time import sleep
from kafka import KafkaProducer

# COMMAND ----------

import sys
import os
import socket
import time

# COMMAND ----------

from confluent_kafka import Producer,SerializingProducer
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient

# COMMAND ----------

topic="69ewibnv-default"

conf = {
        'bootstrap.servers': "sulky-01.srvs.cloudkafka.com:9094,sulky-02.srvs.cloudkafka.com:9094,sulky-03.srvs.cloudkafka.com:9094",#os.environ['CLOUDKARAFKA_BROKERS'],
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'SCRAM-SHA-256',
        'key.serializer':StringSerializer('utf_8'),
        'value.serializer': StringSerializer('utf_8'),
        'sasl.username': "69ewibnv",#%os.environ['CLOUDKARAFKA_USERNAME'],
        'sasl.password': "NqnBhrPCT1bn_lW2O5bTERpQdyQi9Its"#os.environ['CLOUDKARAFKA_PASSWORD']
}
p = SerializingProducer(conf)

# COMMAND ----------

pip install finnhub-python

# COMMAND ----------

import finnhub
import pandas as pd
finnhub_client = finnhub.Client(api_key="c6belnaad3ib9s02bg6g")

# COMMAND ----------


stocks = finnhub_client.stock_symbols('US')
stock_df = pd.DataFrame.from_dict(stocks)
stock_symbols = stock_df["symbol"]
stock_symbols

# COMMAND ----------


i= 0
while True:
  try:
    price = finnhub_client.quote(stock_symbols[i])
    if (price['d'] != None) and (price['d'] != '')  and (price['dp'] != None) and (price['d'] != '') and (price['pc'] != None) and (price['pc'] != ''):
      mydict = {'Name': stock_symbols[i], 'Current_Price':price["c"], 'Delta' :price["d"], 'Delta_perc' :price["dp"], 'Previous_close': price["pc"] }
      print(mydict)
      p.produce(topic,key = "key", value = mydict['Name'] + ", " + str(mydict['Current_Price']) + ", " + str(mydict['Delta']) +", "+ str(mydict['Delta_perc'])+", "+ str(mydict['Previous_close']))
    i = i+1
  except KeyboardInterrupt:
    break
  sleep(8.0)
      

# COMMAND ----------

