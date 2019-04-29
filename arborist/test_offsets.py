import socket
import logging
from time import sleep, time, gmtime
import ujson as json
import re
from threading import Thread, RLock
import sys
import schedule
import os
from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition
import yaml

import arborist.cli as cli

if __name__ == "__main__":

    total_start_time = time()

    # Logging
    logging.basicConfig(
        level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%dT%H:%M:%SZ')
    logging.Formatter.converter = gmtime
    LOGGER = logging.getLogger('arborist')

    # Config
    config = cli.getconfig(sys.argv[1:])
    consumer = Consumer({'bootstrap.servers': ",".join(config['kafka_servers']),
                                  'group.id': config['consumer_group'],
                                  'default.topic.config': {'auto.offset.reset': 'smallest'}})
    
    metadata = consumer.list_topics()
    partitions = metadata.topics['logstash'].partitions.keys()

    start_time = (int(time()) - 60) * 1000
    end_time = start_time + 10 * 1000

    start_topic_partitions = list(
        map(lambda p: TopicPartition('logstash', p, start_time), list(partitions)))
    start_offsets = consumer.offsets_for_times(start_topic_partitions)

    messages = []
    for partition in start_offsets:
      offset_start_time = time()
      consumer.assign([partition])
      current_timestamp = start_time
      running = True
      while current_timestamp < end_time and running == True:
        msg = consumer.poll()
        if not msg.error():
            try:
                data = json.loads(msg.value())
                current_timestamp = msg.timestamp()[1]
            except Exception as e:
                LOGGER.info('Error loading message from kafka.')
                LOGGER.info('Message: %s' % msg.value())
                LOGGER.exception(e)
                exit(1)

            messages.append(data)

        elif msg.error().code() != KafkaError._PARTITION_EOF:
            LOGGER.info(msg.error())
            running = False
        else:
          running = False

      
      print(f"took {time() - offset_start_time} to poll messages for partition {partition.partition}")
    print (f"len messages: {len(messages)}")
    print(f"took {time() - total_start_time} to do everything")
    
    
    

