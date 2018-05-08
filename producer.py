#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Example Kafka Producer.
# Reads lines from stdin and sends to Kafka.
#
import logging
from confluent_kafka import Producer
import sys


broker = 'localhost:9092'
topic = 'qooout'
back_topic = topic
# Producer configuration
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
conf = {'bootstrap.servers': broker}

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
logger.addHandler(handler)


# Create Producer instance
p = Producer(**conf, logger=logger)


# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
def delivery_callback(err, msg):
    if err:
        logger.info('%% Message failed delivery: %s\n' % err)
    else:
        logger.info('%% Message delivered to %s [%d] @ %o\n' %
                         (msg.topic(), msg.partition(), msg.offset()))


def put_next(value):
    sync_produce(topic, value, None)


def msg_retry_time(msg):
    if msg.headers() is not None:
        for h in msg.headers():
            if h[0] == 'retry':
                return int(h[1])
    return 0


def put_back(msg, exc):
    retry_time = msg_retry_time(msg)

    headers = [('retry', str(retry_time + 1))]

    if exc is not None:
        headers.append(('exc', str(exc)))

    if retry_time >= 2:
        logger.info('Three vibrations. Hope human being handle it')
        t = back_topic + '_abort'
    else:
        t = back_topic

    v = msg.value()

    sync_produce(t, v, headers)


def sync_produce(topic, value, headers):
    if headers is None:
        headers = []
        
    p.produce(topic, value, 'qq', callback=delivery_callback, headers=headers)

    p.flush(3)

    if len(p) != 0:
        raise Exception('produce timeout')


