#!/usr/bin/env python
import json
import logging
from pprint import pformat

from confluent_kafka import Consumer, KafkaException, KafkaError

import producer


def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    print('\nKAFKA Stats: {}\n'.format(pformat(stats_json)))


broker = 'localhost:9092'
group = 'wtf'
topics = ['qooin']

# Consumer configuration
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
conf = {'bootstrap.servers': broker, 'group.id': group, 'session.timeout.ms': 6000,
        'enable.auto.commit': False, 'enable.auto.offset.store': True, 'default.topic.config': {'auto.offset.reset': 'smallest'}}


conf['stats_cb'] = stats_cb
conf['statistics.interval.ms'] = 100000

# Create logger for consumer (logs will be emitted when poll() is called)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
logger.addHandler(handler)

# Create Consumer instance
# Hint: try debug='fetch' to generate some log messages
c = Consumer(conf, logger=logger)


def print_assignment(consumer, partitions):
    print('Assignment:', partitions)

c.subscribe(topics, on_assign=print_assignment)

try:
    while True:
        msg = c.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error() is not None:
            # Error or event
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                logger.info('%% %s [%d] reached end at offset %d\n' %
                                 (msg.topic(), msg.partition(), msg.offset()))
                continue
            # unknown kafka error, break the procedure
            raise KafkaException(msg.error())

        # Proper message
        logger.info('%% %s [%d] at offset %d with key %s:\n' %
                         (msg.topic(), msg.partition(), msg.offset(),
                          str(msg.key())))

        try:
            print('Get value {}'.format(msg.value()))
            out = msg.value()[::-1]
            print('header:', msg.headers())

            producer.put_next(out)

        except BaseException as e:
            # If a exception is occurred here, i.e., the procedure cannot put the current message back, the process
            # is teardown by raising the exception. The is because the procedure is claiming C instead of A in the CAP
            # theory.
            logger.exception('Unexpected exception occurred in msg processing. Put it back and handles next one.')
            producer.put_back(msg, e)

        c.commit(message=msg, asynchronous=False)
        print('Commit done: position:', c.position(c.assignment()), 'commit:', c.committed(c.assignment()))

except KeyboardInterrupt:
    logger.info('%% Aborted by user\n')

finally:
    c.close()
