import logging
import json
from time import sleep

import pika
from pika.exceptions import AMQPConnectionError


class SinkDirectQueue:
    def _connect_to_rabbit(self):
        retry_connecting = True
        while retry_connecting:
            try:
                self._connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host='rabbitmq', heartbeat=10000, blocked_connection_timeout=5000)
                )
                retry_connecting = False
            except AMQPConnectionError:
                sleep(2)
                logging.info("Retrying connection to rabbit...")
            except OSError:
                sleep(2)
                logging.info("Retrying connection to rabbit...")

    def __init__(self, data_queue, final_results_queue, metric_name, push_metrics_barrier):
        self._connect_to_rabbit()
        self._channel = self._connection.channel()
        self._data_queue = data_queue
        self._channel.queue_declare(data_queue)
        self._final_results_queue = final_results_queue
        self._channel.queue_declare(final_results_queue)
        self._metric_name = metric_name
        self._push_metrics_barrier = push_metrics_barrier

    def _process_data(self, ch, method, properties, body):
        data = json.loads(body.decode('utf-8'))
        if 'data' in data:
            logging.info("Received results for metric: {}".format(self._metric_name))
            self._push_metrics_barrier.wait()
            self._channel.basic_publish(
                exchange='',
                routing_key=self._final_results_queue,
                body=bytes(json.dumps({self._metric_name: data['data']}), encoding='utf-8'),
                properties=pika.BasicProperties(delivery_mode=2)
            )

    def start(self):
        self._channel.basic_consume(queue=self._data_queue, on_message_callback=self._process_data)
        self._channel.start_consuming()
