# Databricks notebook source
!pip install confluent_kafka

# COMMAND ----------

import sys
import os
import socket
import time
from confluent_kafka import Consumer, KafkaException, KafkaError
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringDeserializer

# COMMAND ----------

topics=["69ewibnv-default"]

# Consumer configuration
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

conf = {
  'bootstrap.servers': "sulky-01.srvs.cloudkafka.com:9094,sulky-02.srvs.cloudkafka.com:9094,sulky-03.srvs.cloudkafka.com:9094",#os.environ['CLOUDKARAFKA_BROKERS'],
  'group.id': "%sss-consumergfd-69ewibnv",
  'session.timeout.ms': 6000,
  'default.topic.config': {'auto.offset.reset': 'largest'},
  'security.protocol': 'SASL_SSL',
  'sasl.mechanisms': 'SCRAM-SHA-256',
  'key.deserializer':StringDeserializer('utf_8'),
  'value.deserializer': StringDeserializer('utf_8'),
  'sasl.username': "69ewibnv",#%os.environ['CLOUDKARAFKA_USERNAME'],
  'sasl.password': "NqnBhrPCT1bn_lW2O5bTERpQdyQi9Its"#os.environ['CLOUDKARAFKA_PASSWORD']
}

c = DeserializingConsumer(conf)
c.subscribe(topics)

# COMMAND ----------

host = 'localhost'
port = 12349
i = 0
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((host, port))
s.listen(1)

try:
    while True:
        print("waiting...")
        conn, addr = s.accept()
        print("it's been arrived...")
        try:
            while True:
                try:
                    msg = c.poll(1.0)
                    if msg is None:
                        print("nothing came...")
                        continue

                    user = msg.value()
                    if user is not None:
                        print(user)
                        conn.send(bytes("{}\n".format(user), "utf-8"))
                except KeyboardInterrupt:
                        break
            conn.close()
        except socket.error: pass
finally:
    s.close()

# COMMAND ----------


try:
    while True:
        print("waiting...")
        #conn, addr = s.accept()
        print("it's been arrived...")
        try:
            while True:
                msg = c.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    # Error or event
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        # Error
                        raise KafkaException(msg.error())
                else:
                    # Proper message
                    #sys.stderr.write(str(msg.key()))
                    print(msg.value())
                    value = msg.value()
                    #conn.send(bytes("{}\n".format(value), "utf-8"))
        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')
            conn.close()
        except socket.error: pass
finally:
    s.close()

# COMMAND ----------

