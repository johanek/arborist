''' Arborist '''

import socket
import logging
from time import sleep, time, gmtime
from datetime import datetime, timedelta
import ujson as json
import re
from threading import Thread, RLock
import sys
import schedule
import os
from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition
import yaml
from redis import Redis
import pickle

import arborist.cli as cli
from arborist.rules_engine import StreamRules

LOGGER = logging.getLogger('arborist')


class Arborist(object):
    ''' main arborist class'''

    def __init__(self, config):
        self.config = config
        self.debug = config['debug']
        self.consumer = Consumer({
            'bootstrap.servers':
            ",".join(self.config['kafka_servers']),
            'group.id':
            self.config['consumer_group'],
            'default.topic.config': {
                'auto.offset.reset': 'smallest'
            }
        })
        self.consumer.subscribe(self.config['log_consumer_topics'])
        self.redis = Redis()
        # self.producer = Producer(
        #     {'bootstrap.servers': ",".join(self.config['kafka_servers'])})

        # Setup thread to run kafka consumer
        kafka_thread = Thread(target=self.kafka_worker, args=())
        kafka_thread.daemon = True
        kafka_thread.start()

    def kafka_worker(self):
        ''' Main thread to constantly consume and process records '''
        LOGGER.info('starting arborist worker')
        consumer = Consumer({
            'bootstrap.servers':
            ",".join(self.config['kafka_servers']),
            'group.id':
            self.config['consumer_group'],
            'default.topic.config': {
                'auto.offset.reset': 'smallest'
            }
        })
        consumer.subscribe(self.config['log_consumer_topics'])

        running = True
        start_time = self.roundTime(datetime.now(), 10)
        end_time = start_time + timedelta(seconds=10)
        message_bucket = []
        while running:
            msg = consumer.poll()
            if not msg.error():
                try:
                    data = json.loads(msg.value())
                    data['_offset'] = msg.offset()
                    message_bucket.append(data)
                except Exception as e:
                    LOGGER.info('Error loading message from kafka.')
                    LOGGER.info('Message: %s' % msg.value())
                    LOGGER.exception(e)
                    exit(1)

            elif msg.error().code() != KafkaError._PARTITION_EOF:
                LOGGER.info(msg.error())
                running = False

            if datetime.now() > end_time:
                self.write_to_cache(message_bucket, start_time)
                start_time = end_time
                end_time = start_time + timedelta(seconds=10)
                message_bucket = []

        LOGGER.info('Arborist worker finished')

    def write_to_cache(self, messages, start_time):
        self.redis.set(
            int(start_time.timestamp()), pickle.dumps(messages), ex=300)
        LOGGER.info("wrote {} messages to redis for time bucket {}".format(
            len(messages), start_time.isoformat()))

    def read_from_cache(self, name, window):
        LOGGER.info("called read_from_cache {} {}".format(name, window))
        start_time = int(datetime.now().timestamp()) - window
        messages = []
        for key in self.redis.keys():
            if int(key) > start_time:
                data = self.redis.get(key)
                messages = messages + pickle.loads(data)
        return messages

    def roundTime(self, dt=None, roundTo=60):
        """Round a datetime object to any time lapse in seconds
        dt : datetime.datetime object, default now.
        roundTo : Closest number of seconds to round to, default 1 minute.
        Author: Thierry Husson 2012 - Use it as you want but don't blame me.
        """
        if dt == None: dt = datetime.now()
        seconds = (dt.replace(tzinfo=None) - dt.min).seconds
        rounding = (seconds + roundTo / 2) // roundTo * roundTo
        return dt + timedelta(0, rounding - seconds, -dt.microsecond)

    def rules_worker(self, rule_config):
        schedule.every(rule_config['interval']).seconds.do(
            StreamRules.process, arbor_instance=self, rule=rule_config)
        while True:
            schedule.run_pending()
            sleep(0.1)


if __name__ == "__main__":
    # Logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%SZ')
    logging.Formatter.converter = gmtime
    LOGGER = logging.getLogger('arborist')

    # Config
    config = cli.getconfig(sys.argv[1:])
    arbor = Arborist(config=config)

    # Rules
    rule_path = 'rules'
    rule_files = os.listdir(rule_path)
    rule_threads = {}
    for file in rule_files:
        filename = '{}/{}'.format(rule_path, file)
        with open(filename, 'r') as file:
            rule_config = yaml.load(file, Loader=yaml.FullLoader)
            rule_threads[rule_config['name']] = Thread(
                target=arbor.rules_worker, args=[rule_config])
            rule_threads[rule_config['name']].daemon = True
            rule_threads[rule_config['name']].start()

    while True:
        sleep(0.1)
