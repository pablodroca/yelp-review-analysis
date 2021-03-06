import logging
import json
from time import sleep

import pika
from pika.exceptions import AMQPConnectionError


class Reducer:
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
            except OSError:
                sleep(2)
                logging.info("Retrying connection to rabbit...")

    def __init__(self, aggregated_data_queue_name, sink_queue_name, aggregators_quantity, unflatten_key,
                 unflatten_value_key):
        self._connect_to_rabbit()
        self._reviews_path = aggregated_data_queue_name
        self._channel = self._connection.channel()
        self._aggregated_data_queue_name = aggregated_data_queue_name
        self._channel.queue_declare(queue=aggregated_data_queue_name)
        self._channel = self._connection.channel()
        self._sink_queue_name = sink_queue_name
        self._channel.queue_declare(queue=sink_queue_name)
        self._received_aggregator_data_messages = 0
        self._aggregators_quantity = aggregators_quantity
        self._unflatten_key = unflatten_key
        self._unflatten_value_key = unflatten_value_key
        self._aggregation = {}

    def _flush_data(self):
        unflattened_data = []
        for key, value in self._aggregation.items():
            unflattened_data.append({
                self._unflatten_key: key,
                self._unflatten_value_key: value
            })

        data_bytes = bytes(json.dumps({'type': 'data', 'data': unflattened_data}), encoding='utf-8')
        flush_data_notif_bytes = bytes(json.dumps({'type': 'flush'}), encoding='utf-8')

        self._channel.basic_publish(
            exchange='',
            routing_key=self._sink_queue_name,
            body=data_bytes,
            properties=pika.BasicProperties(expiration="900000")
        )
        self._channel.basic_publish(
            exchange='',
            routing_key=self._sink_queue_name,
            body=flush_data_notif_bytes,
            properties=pika.BasicProperties(expiration="900000")
        )

        logging.info("Sending reduced data.")

        self._aggregation = {}
        self._received_aggregator_data_messages = 0

    def _process_data_chunk(self, data_aggregation):
        for key, value in data_aggregation.items():
            if key in self._aggregation:
                self._aggregation[key] += value
            else:
                self._aggregation[key] = value

    def _process_data(self, ch, method, properties, body):
        data_aggregation = json.loads(body.decode('utf-8'))
        self._received_aggregator_data_messages += 1
        self._process_data_chunk(data_aggregation['data'])
        if self._received_aggregator_data_messages == self._aggregators_quantity:
            self._flush_data()

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self._channel.basic_consume(queue=self._aggregated_data_queue_name,
                                    on_message_callback=self._process_data)
        self._channel.start_consuming()
