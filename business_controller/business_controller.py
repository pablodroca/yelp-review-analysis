import json
import logging

import pika


class BusinessController:
    def __init__(self, business_queue, business_exchange, business_message_size, exchange_requests):
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq')
        )
        self._business_queue_channel = self._connection.channel()
        self._business_queue_channel.exchange_declare(exchange=exchange_requests, exchange_type='direct')

        self._business_queue_name = business_queue
        self._business_queue_channel.queue_declare(queue=business_queue)
        self._business_queue_channel.queue_bind(exchange=exchange_requests, queue=business_queue,
                                                routing_key=business_queue)

        self._business_joiners_channel = self._connection.channel()
        self._business_joiners_channel.exchange_declare(exchange=business_exchange, exchange_type='fanout')

        self._business_exchange = business_exchange
        self._business_message_size = business_message_size

        self._current_businesses = []

    def _flush_data(self):
        self._send_businesses_to_joiners(self._current_businesses)
        self._current_businesses = []

        data_bytes = bytes(json.dumps({'type:', 'flush'}), encoding='utf-8')

        self._business_joiners_channel.basic_publish(
            exchange=self._business_exchange,
            routing_key='',
            body=data_bytes
        )

        logging.info("Finishing processing businesses data.")

    def _send_businesses_to_joiners(self, businesses):
        data_bytes = bytes(json.dumps(
            {'type': 'data',
             'data': [
                 {'business_id': business['business_id'],
                  'city': business['city']
                  } for business in businesses]
             }
        ), encoding='utf-8')

        self._business_joiners_channel.basic_publish(
            exchange=self._business_exchange,
            routing_key='',
            body=data_bytes
        )

    def _process_data_chunk(self, received_businesses):
        if len(self._current_businesses) + len(received_businesses) >= self._business_message_size:
            total_businesses_to_append = self._business_message_size - len(self._current_businesses)
            propagation_businesses = self._current_businesses + received_businesses[:total_businesses_to_append]
            self._current_businesses = received_businesses[total_businesses_to_append:]
            self._send_businesses_to_joiners(propagation_businesses)
        else:
            self._current_businesses += received_businesses

    def _process_data(self, ch, method, properties, body):
        businesses = json.loads(body.decode('utf-8'))
        if businesses['type'] == 'data':
            self._process_data_chunk(businesses['data'])
        else:
            self._flush_data()

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self._business_queue_channel.basic_consume(queue=self._business_queue_name,
                                                   on_message_callback=self._process_data)
        logging.info("Starting consuming.")
        self._business_queue_channel.start_consuming()
