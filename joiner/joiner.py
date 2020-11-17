import logging
import json

import pika


class Joiner:
    def __init__(self, aggregated_data_queue_name, sink_queue_name, aggregators_quantity):
        self._reviews_path = aggregated_data_queue_name

        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq')
        )
        self._aggregated_data_channel = self._connection.channel()
        self._aggregated_data_queue_name = aggregated_data_queue_name
        self._aggregated_data_channel.queue_declare(queue=aggregated_data_queue_name)
        self._sink_queue_channel = self._connection.channel()
        self._sink_queue_name = sink_queue_name
        self._sink_queue_channel.queue_declare(queue=sink_queue_name)
        self._received_aggregator_data_messages = 0
        self._aggregators_quantity = aggregators_quantity
        self._aggregation = {}

    def _flush_data(self):
        data_bytes = bytes(json.dumps({
            'type': 'data',
            'data': self._aggregation
        }), encoding='utf-8')

        logging.info('Finishing processing reducing. Results: {}'.format(self._aggregation))

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
        logging.info(data_aggregation)
        self._received_aggregator_data_messages += 1
        self._process_data_chunk(data_aggregation['data'])
        if self._received_aggregator_data_messages == self._aggregators_quantity:
            self._flush_data()

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self._aggregated_data_channel.basic_consume(queue=self._aggregated_data_queue_name,
                                                    on_message_callback=self._process_data)
        self._aggregated_data_channel.start_consuming()
