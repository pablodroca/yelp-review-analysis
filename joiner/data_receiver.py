import json
import logging
import pika


class DataReceiver:
    def __init__(self, critical_data_exchange):
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq')
        )

        self._channel = self._connection.channel()
        self._channel.exchange_declare(exchange=critical_data_exchange, exchange_type='fanout')
        self._queue_name = self._channel.queue_declare(queue='', exclusive=True).method.queue
        self._channel.queue_bind(exchange=critical_data_exchange, queue=self._queue_name)

    def _process_data(self, ch, method, properties, body):
        businesses = json.loads(body.decode('utf-8'))
        logging.info(businesses)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self._channel.basic_consume(queue=self._queue_name, on_message_callback=self._process_data)
