import json
import logging
from time import sleep

import pika
from pika.exceptions import AMQPConnectionError


class DataReceiver:
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

    def __init__(self, critical_data_exchange, table_to_fill, table_filled_semaphore, join_key):
        self._connect_to_rabbit()

        self._channel = self._connection.channel()
        self._channel.exchange_declare(exchange=critical_data_exchange, exchange_type='fanout')
        self._queue_name = self._channel.queue_declare(queue='', exclusive=True).method.queue
        self._channel.queue_bind(exchange=critical_data_exchange, queue=self._queue_name)
        self._table_to_fill = table_to_fill
        self._table_filled_semaphore = table_filled_semaphore
        self._join_key = join_key
        self._consumer_tag = None

    def _process_data_chunk(self, registers):
        for register in registers:
            self._table_to_fill[register[self._join_key]] = register

    def _process_data(self, ch, method, properties, body):
        end_data_message = False
        data_chunk = json.loads(body.decode('utf-8'))
        if data_chunk['type'] == 'data':
            self._process_data_chunk(data_chunk['data'])
        else:
            logging.info("Finishing processing data")
            self._table_filled_semaphore.release()
            end_data_message = True

        ch.basic_ack(delivery_tag=method.delivery_tag)

        if end_data_message:
            logging.info("Waiting to process another stream.")
            self._table_filled_semaphore.acquire()
            logging.info("Acquired semaphore to listen for other stream. Flushing old table.")
            keys = self._table_to_fill.keys()
            for key in keys:
                del self._table_to_fill[key]

    def start(self):
        logging.info("Starting to listen for critical data for joining.")
        self._consumer_tag = self._channel.basic_consume(queue=self._queue_name, on_message_callback=self._process_data)
        self._channel.start_consuming()
