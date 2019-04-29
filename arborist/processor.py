''' Arborist '''

import socket
import logging
from time import sleep, time, gmtime
import ujson as json
import re
from threading import Thread, RLock
import sys
import schedule
import os
from confluent_kafka import Producer, Consumer, KafkaError
import yaml

import arborist.cli as cli
from arborist.cache import CacheList
from arborist.rules_engine import StreamRules

LOGGER = logging.getLogger('arborist')

class Arborist(object):
    ''' main arborist class'''

    def __init__(self, config, cache):
        self.config = config
        self.cache = cache
        self.debug = config['debug']
        self.producer = Producer(
            {'bootstrap.servers': ",".join(self.config['kafka_servers'])})

        # Setup thread to run kafka consumer
        kafka_thread = Thread(target=self.kafka_worker, args=())
        kafka_thread.daemon = True
        kafka_thread.start()

    def kafka_worker(self):
        ''' Main thread to constantly consume and process records '''
        LOGGER.info('starting arborist worker')
        consumer = Consumer({'bootstrap.servers': ",".join(self.config['kafka_servers']),
                                  'group.id': self.config['consumer_group'],
                                  'default.topic.config': {'auto.offset.reset': 'smallest'}})
        consumer.subscribe(self.config['log_consumer_topics'])

        running = True
        while running:
            msg = consumer.poll()
            if not msg.error():
                try:
                    data = json.loads(msg.value())
                    data['_offset'] = msg.offset()
                except Exception as e:
                    LOGGER.info('Error loading message from kafka.')
                    LOGGER.info('Message: %s' % msg.value())
                    LOGGER.exception(e)
                    exit(1)

                self.cache.add_entry(data, ttl=300)

            elif msg.error().code() != KafkaError._PARTITION_EOF:
                LOGGER.info(msg.error())
                running = False

        LOGGER.info('Arborist worker finished')
    






if __name__ == "__main__":
    # Logging
    logging.basicConfig(
        level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%dT%H:%M:%SZ')
    logging.Formatter.converter = gmtime
    LOGGER = logging.getLogger('arborist')

    # Config
    config = cli.getconfig(sys.argv[1:])
    cache = CacheList()
    arbor = Arborist(config=config, cache=cache)

    # Rules
    rule_path = 'rules'
    rule_files = os.listdir(rule_path)
    for file in rule_files:
        filename = '{}/{}'.format(rule_path, file)
        with open(filename, 'r') as file:
            rule_config = yaml.load(file, Loader=yaml.FullLoader)
            schedule.every(rule_config['interval']).seconds.do(StreamRules.process, cache=cache, maxage=rule_config['window'], rule=rule_config)

    while True:
        schedule.run_pending()
        sleep(0.1)
