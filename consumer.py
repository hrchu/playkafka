#!/usr/bin/env python
from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import json
import logging
from pprint import pformat

from playkafka import producer


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
logger = logging.getLogger('consumer')
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
        if msg.error():
            # Error or event
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                 (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                # unknown kafka error, teardown the procedure
                raise KafkaException(msg.error())
        else:
            # Proper message
            sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                             (msg.topic(), msg.partition(), msg.offset(),
                              str(msg.key())))

            # blah
            out = msg.value()[::-1]

            # produce it to the next stage
            producer.produce(out)

            print('value', msg.value())
            c.commit(message= msg, asynchronous=False)
            print('position:', c.position(c.assignment()), 'commit:', c.committed(c.assignment()))

except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')

finally:
    c.close()
