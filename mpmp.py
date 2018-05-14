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

def profile_server(job_profile):
    # [name, desired_number, procedure_instance, [(pid, conn), (pid, conn)...]]
    job_workers = []
    for name, profile in job_profile.items():
        number = profile.get('number')
        args = profile.get('procedure')
        p = Procedure(args.get('in'), args.get('out'), args.get('func'), args.get('conf'))
        job_workers.append([name, number, p, []])

    q = None

    try:
        while True:
            time.sleep(3)

            if q is None or not q.is_alive():
                q, q_conn = init_query_worker()

            if q_conn.poll():
                req = q_conn.recv()
                try:
                    job_type, desired_number = str(req).split()
                    for state in job_workers:
                        if state[0] == job_type:
                            state[1] = int(desired_number)
                            break

                except ValueError as e:
                    logging.warning("unknown query command")
                    # raise e

            for state in job_workers:
                plist = state[3]

                for p, conn in plist:
                    logger.info(p.name + 'is' + str(p.is_alive()))
                    if not p.is_alive():
                        logger.info(p.name + ' is dead!')
                        plist.remove((p, conn))

                p_count = state[1]
                f = state[2].run
                while len(plist) < p_count:
                    p_conn, c_conn = Pipe()
                    p = Process(target=f, args=(c_conn,))
                    original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
                    p.start()
                    signal.signal(signal.SIGINT, original_sigint_handler)
                    plist.append((p, p_conn))
                    logger.info('spwan ' + p.name)

                while len(plist) > p_count:
                    p, conn = plist.pop()
                    conn.send('kill')
                    p.join()
                    logger.info('kill ' + p.name)

    except KeyboardInterrupt:
        logging.info('KeyboardInterrupt')
    finally:
        q.terminate()

        for state in job_workers:
            plist = state[3]
            for p, conn in plist:
                # p.terminate()

                conn.send('kill')
                p.join()
                logger.info('kill ' + p.name)

# def server(f):
#     p_count = 2
#     plist = []
#     q = None
#
#     try:
#         while True:
#             time.sleep(3)
#
#             if q is None or not q.is_alive():
#                 q, q_conn = init_query_worker()
#
#             if q_conn.poll():
#                 req = q_conn.recv()
#                 try:
#                     p_count = int(req)
#                 except ValueError as e:
#                     logging.warning("unknown query command")
#                     # raise e
#
#             for p, conn in plist:
#                 logger.info(p.name + 'is' + str(p.is_alive()))
#                 if not p.is_alive():
#                     logger.info(p.name + ' is dead!')
#                     plist.remove((p, conn))
#
#             while len(plist) < p_count:
#                 p_conn, c_conn = Pipe()
#                 p = Process(target=f, args=(c_conn,))
#                 original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
#                 p.start()
#                 signal.signal(signal.SIGINT, original_sigint_handler)
#                 plist.append((p, p_conn))
#                 logger.info('spwan ' + p.name)
#
#             while len(plist) > p_count:
#                 p, conn = plist.pop()
#                 conn.send('kill')
#                 p.join()
#                 logger.info('kill ' + p.name)
#
#     # except KeyboardInterrupt:
#     finally:
#         q.terminate()
#
#         for p, conn in plist:
#             # p.terminate()
#
#             conn.send('kill')
#             p.join()
#             logger.info('kill ' + p.name)


if __name__ == '__main__':
    def uppercase(msg):
        return str(msg.value()).upper()

    def reverse(msg):
        return str(msg.value())[::-1]

    conf = {'bootstrap.servers': 'localhost:19092, localhost:29092, localhost:39092'}

    job_profile = {
        'foo-bar': {
            'number': 2,
            'procedure': {
                'in': 'foo',
                'out': 'bar',
                'func': uppercase,
                'conf': conf
            }

        },
        'bar-baz': {
            'number': 2,
            'procedure': {
                'in': 'bar',
                'out': 'baz',
                'func': reverse,
                'conf': conf
            }
        },
    }





    profile_server(job_profile)


    # myc1 = Procedure('foo2', 'bar2', uppercase, conf)
    # myc2 = Procedure('bar', 'baz', uppercase, conf)
    # server(myc1.run)
    # server(myc2.run)

