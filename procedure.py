#!/usr/bin/env python
import json
import logging
import multiprocessing
from pprint import pformat

from confluent_kafka import Consumer, KafkaException, KafkaError
from confluent_kafka.cimpl import Producer

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
# handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
handler.setFormatter(logging.Formatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s','%m-%d %H:%M:%S'))
logger.addHandler(handler)


class Procedure:
    def __init__(self, in_topic, out_topic, func, kafka_conf=None):
        self._in_topic = in_topic
        self._out_topic = out_topic
        self._abort_topic = in_topic + '_abort'
        self._func = func

        def init_kafka():
            def stats_cb(stats_json_str):
                stats_json = json.loads(stats_json_str)
                print('\nKAFKA Stats: {}\n'.format(pformat(stats_json)))

            # FIXME: not working
            def commit_cb(err, ps):
                print('on_commit: err %s, partitions %s' % (err, ps))

            conf = {'bootstrap.servers': 'localhost:9092',
                    'group.id': 'proc_default',
                    'session.timeout.ms': 6000,
                    'enable.auto.commit': False,
                    'enable.auto.offset.store': True,
                    'default.topic.config': {'auto.offset.reset': 'smallest'},
                    'on_commit': lambda err, ps: commit_cb(err, ps),
                    'stats_cb': stats_cb,
                    'statistics.interval.ms': 100000
                    }

            if kafka_conf:
                conf.update(kafka_conf)

            # Hint: try debug='fetch' to generate some log messages
            self.consumer = Consumer(conf, logger=logger)
            _conf = conf.copy()
            del _conf['on_commit']
            self.producer = Producer(_conf)

        init_kafka()

    def run(self):
        print('qq')
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
            blocking_produce(self._out_topic, value)

        def msg_retry_time(msg):
            if msg.headers() is not None:
                for h in msg.headers():
                    if h[0] == 'retry':
                        return int(h[1])
            return 0

        def put_back(msg, exc=None):
            retry_time = msg_retry_time(msg)

            headers = [('retry', str(retry_time + 1))]

            if exc is not None:
                headers.append(('exc', str(exc)))

            if retry_time >= 2:
                logger.info('Three vibrations. Hope human being handle it')
                t = self._abort_topic
            else:
                t = self._in_topic

            v = msg.value()

            blocking_produce(t, v, headers)

        def blocking_produce(topic, value, headers=[]):
            p = self.producer

            p.produce(topic, value, 'qq', callback=delivery_callback, headers=headers)

            p.flush(3)

            if len(p) != 0:
                raise Exception('produce timeout')


        ###################################################

        c = self.consumer

        def print_assignment(consumer, partitions):
            print('Assignment:', partitions)

        c.subscribe([self._in_topic], on_assign=print_assignment)

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
                    print('Get header: {} value: {}'.format(msg.headers(), msg.value()))

                    out = self._func(msg)

                    # raise Exception('check point')
                    put_next(out)

                except BaseException as e:
                    # If a exception is occurred here, i.e., the procedure cannot put the current message back,
                    # the process is halt by raising the exception. The is because the procedure is claiming C
                    # instead of A in the CAP theory.
                    logger.exception(
                        'Unexpected exception occurred in msg processing. Put it back and handles next one.')

                    # raise Exception('check point')
                    put_back(msg, e)

                c.commit(message=msg, asynchronous=False)
                print('Commit done: position:', c.position(c.assignment()), 'commit:', c.committed(c.assignment()))

        except KeyboardInterrupt:
            logger.info('%% Aborted by user\n')

        finally:
            c.close()


if __name__ == "__main__":
    def uppercase(msg):
        return str(msg.value()).upper()

    conf = {'bootstrap.servers': 'localhost:19092, localhost:29092, localhost:39092'}
    myc = Procedure('foo', 'bar', uppercase, conf)
    myc.run()

    # p = multiprocessing.Process(target=myc.run)
    # p.start()
    # p.join()
