# -*- coding: utf-8 -*-
import os
import signal
from multiprocessing import Process, Pipe

import time

import logging

import query
from procedure import Procedure

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
# handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
handler.setFormatter(logging.Formatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s','%m-%d %H:%M:%S'))
logger.addHandler(handler)

# TODO: deamon


def f(conn):
    while True:
        try:
            time.sleep(3)
            if conn.poll() and conn.recv() == 'kill':
                logger.info('be killed ' + os.name)
                break
            logger.info('iam' + str(os.getpid()))
        except KeyboardInterrupt:
            print('Ctrl C')


def init_query_worker():
    q_conn, c_conn = Pipe()
    q = Process(target=query.run, args=(c_conn,))
    original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    q.start()
    signal.signal(signal.SIGINT, original_sigint_handler)
    return q, q_conn

def server(f):
    p_count = 3
    plist = []
    q = None

    try:
        while True:
            time.sleep(3)

            if q is None or not q.is_alive():
                q, q_conn = init_query_worker()

            if q_conn.poll():
                req = q_conn.recv()
                p_count = int(req)

            for p, conn in plist:
                logger.info(p.name + 'is' + str(p.is_alive()))
                if not p.is_alive():
                    logger.info(p.name + ' is dead!')
                    plist.remove((p, conn))

            while len(plist) < p_count:
                p_conn, c_conn = Pipe()
                p = Process(target=f, args=(c_conn,))
                original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
                p.start()
                signal.signal(signal.SIGINT, original_sigint_handler)
                plist.append((p, p_conn))
                logger.info('spwan ' + p.name)

            # while len(plist) > p_count:
            #     p, conn = plist.pop()
            #     conn.send('kill')
            #     p.join()
            #     logger.info('kill ' + p.name)

    except KeyboardInterrupt:
        q.terminate()

        for p, conn in plist:
            p.terminate()
            # conn.send('kill')
            # p.join()
            logger.info('kill ' + p.name)

if __name__ == '__main__':
    # def uppercase(msg):
    #     return str(msg.value()).upper()
    #
    # conf = {'bootstrap.servers': 'localhost:19092, localhost:29092, localhost:39092'}
    # myc = Procedure('foo', 'bar', uppercase, conf)

    server(f)