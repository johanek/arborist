''' Streamric '''

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
from confluent_kafka import Consumer, KafkaError, TopicPartition
import yaml

import streamric.cli as cli
from streamric.cache import StreamCache

LOGGER = logging.getLogger('streamric')

class Stream(object):
    ''' main streamric class'''

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
        self.cache = StreamCache()

        # Setup thread to run kafka consumer
        kafka_thread = Thread(target=self.kafka_worker, args=())
        kafka_thread.daemon = True
        kafka_thread.start()

    def kafka_worker(self):
        ''' Main thread to constantly consume and process records '''
        LOGGER.info('starting streamric kafka worker')
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
                self.cache.write_to_cache(message_bucket, start_time)
                start_time = end_time
                end_time = start_time + timedelta(seconds=10)
                message_bucket = []

        LOGGER.info('streamric worker finished')

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

    def run_threaded(self, job_func, args):
        job_thread = Thread(target=job_func, args=[args])
        job_thread.start()