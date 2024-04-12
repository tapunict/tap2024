#!/usr/bin/env python
# From https://medium.com/@philamersune/bypassing-the-gil-a-concurrent-kafka-consumer-dcc44ae9260a
import logging
import os
import threading
import time
from multiprocessing import Process
from queue import Queue

from confluent_kafka import Consumer


def _process_msg(q, c):
    msg = q.get(timeout=60)  # Set timeout to care for POSIX<3.0 and Windows.
    logging.info(
        '#%sT%s - Received message: %s',
        os.getpid(), threading.get_ident(), msg.value().decode('utf-8')
    )
    time.sleep(5)
    q.task_done()
    c.commit(msg)


def _consume(config):
    logging.info(
        '#%s - Starting consumer group=%s, topic=%s',
        os.getpid(), config['kafka_kwargs']['group.id'], config['topic'],
    )
    c = Consumer(**config['kafka_kwargs'])
    c.subscribe([config['topic']])
    q = Queue(maxsize=config['num_threads'])

    while True:
        logging.info('#%s - Waiting for message...', os.getpid())
        try:
            msg = c.poll(60)
            if msg is None:
                continue
            if msg.error():
                logging.error(
                    '#%s - Consumer error: %s', os.getpid(), msg.error()
                )
                continue
            q.put(msg)
            # Use default daemon=False to stop threads gracefully in order to
            # release resources properly.
            t = threading.Thread(target=_process_msg, args=(q, c))
            t.start()
        except Exception:
            logging.exception('#%s - Worker terminated.', os.getpid())
            c.close()


def main(config):
    """
    Simple program that consumes messages from Kafka topic and prints to
    STDOUT.
    """
    workers = []
    while True:
        num_alive = len([w for w in workers if w.is_alive()])
        if config['num_workers'] == num_alive:
            continue
        for _ in range(config['num_workers']-num_alive):
            p = Process(target=_consume, daemon=True, args=(config,))
            p.start()
            workers.append(p)
            logging.info('Starting worker #%s', p.pid)


if __name__ == '__main__':
    logging.basicConfig(
        level=getattr(logging, os.getenv('LOGLEVEL', '').upper(), 'INFO'),
        format='[%(asctime)s] %(levelname)s:%(name)s:%(message)s',
    )
    main(config={
        # At most, this should be the total number of Kafka partitions on
        # the topic.
        'num_workers': 2,
        'num_threads': 16,
        'topic': 'tap',
        'kafka_kwargs': {
            'bootstrap.servers': 'kafkaServer:9092',
            'group.id': 'my_consumer_group',
            'auto.offset.reset': 'earliest',
            # Commit manually to care for abrupt shutdown.
            'enable.auto.commit': False,
        },
    })