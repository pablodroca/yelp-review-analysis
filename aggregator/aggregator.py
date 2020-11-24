import json
import logging
from time import sleep

import pika
from pika.exceptions import AMQPConnectionError


class Aggregator:
    def _connect_to_rabbit(self):
        retry_connecting = True
        while retry_connecting:
            try:
                self._connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host='rabbitmq')
                )
                retry_connecting = False
            except AMQPConnectionError:
                logging.info("Rabbit is not ready yet...")
                sleep(2)
                logging.info("Retrying connection to rabbit...")
            except OSError:
                sleep(2)
                logging.info("Retrying connection to rabbit...")

    def __init__(self, source_queue_name, reducer_queue_name, key):
        self._connect_to_rabbit()
        self._channel = self._connection.channel()
        self._source_queue_name = source_queue_name
        self._channel.queue_declare(queue=source_queue_name)
        self._reducer_queue_name = reducer_queue_name
        self._channel.queue_declare(queue=reducer_queue_name)
        self._key = key
        self._counter = {}

    def _flush_data(self):
        data_bytes = bytes(json.dumps({
            'type': 'data',
            'data': self._counter
        }), encoding='utf-8')

        self._channel.basic_publish(
            exchange='',
            routing_key=self._reducer_queue_name,
            body=data_bytes,
            properties=pika.BasicProperties(delivery_mode=2)
        )

        self._counter = {}

    def _process_data_chunk(self, data_chunk):
        for register in data_chunk:
            if register[self._key] in self._counter:
                self._counter[register[self._key]] += 1
            else:
                self._counter[register[self._key]] = 1

    def _process_data(self, ch, method, properties, body):
        data_chunk = json.loads(body.decode('utf-8'))
        if data_chunk['type'] == 'data':
            self._process_data_chunk(data_chunk['data'])
        else:
            self._flush_data()

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self._channel.basic_consume(queue=self._source_queue_name,
                                    on_message_callback=self._process_data)
        self._channel.start_consuming()
