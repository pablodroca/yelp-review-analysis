import json
import logging

import pika


class Aggregator:
    def __init__(self, source_queue_name, reducer_queue_name):
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq')
        )
        self._source_channel = self._connection.channel()
        self._source_queue_name = source_queue_name
        self._source_channel.queue_declare(queue=source_queue_name)
        self._reducer_channel = self._connection.channel()
        self._reducer_queue_name = reducer_queue_name
        self._reducer_channel.queue_declare(queue=reducer_queue_name)
        self._counter = {}

    def _flush_data(self):
        data_bytes = bytes(json.dumps({
            'type': 'data',
            'data': self._counter
        }), encoding='utf-8')

        logging.info('Finishing processing aggregation. Results: {}'.format(self._counter))

        self._reducer_channel.basic_publish(
            exchange='',
            routing_key=self._reducer_queue_name,
            body=data_bytes,
            properties=pika.BasicProperties(delivery_mode=2)
        )

        self._counter = {}

    def _process_data_chunk(self, data_chunk):
        for data in data_chunk:
            if data in self._counter:
                self._counter[data] += 1
            else:
                self._counter[data] = 1

    def _process_data(self, ch, method, properties, body):
        data_chunk = json.loads(body.decode('utf-8'))
        if data_chunk['type'] == 'data':
            self._process_data_chunk(data_chunk['data'])
        else:
            self._flush_data()

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self._source_channel.basic_consume(queue=self._source_queue_name,
                                           on_message_callback=self._process_data)
        self._source_channel.start_consuming()
