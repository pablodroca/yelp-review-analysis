import datetime
import json
import logging
from time import sleep

import pika
from pika.exceptions import AMQPConnectionError


class Sorter:
    def _connect_to_rabbit(self):
        retry_connecting = True
        while retry_connecting:
            try:
                self._connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host='rabbitmq')
                )
                retry_connecting = False
            except AMQPConnectionError:
                sleep(2)
                logging.info("Retrying connection to rabbit...")

    def __init__(self, data_queue, sink_queue, quantity_to_take):
        self._connect_to_rabbit()
        self._channel = self._connection.channel()
        self._data_queue = data_queue
        self._channel.queue_declare(queue=data_queue)
        self._quantity_to_take = quantity_to_take
        self._channel.queue_declare(queue=sink_queue)
        self._sink_queue = sink_queue

    def _send_sorted_data(self, data):
        sorted_data = sorted(data, key=lambda x: x['total_funny_reviews'], reverse=True)[:self._quantity_to_take]
        logging.info("Sending sorted data: {}".format(sorted_data))
        self._channel.basic_publish(
            exchange='',
            routing_key=self._sink_queue,
            body=bytes(json.dumps({'type': 'data', 'data': sorted_data}), encoding='utf-8')
        )

    def _process_data(self, ch, method, properties, body):
        data = json.loads(body.decode('utf-8'))
        if data['type'] == 'data':
            self._send_sorted_data(data['data'])
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self._channel.basic_consume(queue=self._data_queue,
                                    on_message_callback=self._process_data)
        self._channel.start_consuming()
