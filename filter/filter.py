import logging
import json
from time import sleep

import pika
from pika.exceptions import AMQPConnectionError


class Filter:
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

    def __init__(self, data_queue, sink_exchange, filter_operation, filter_key=None, filter_parameter=None,
                 filter_key_1=None, filter_key_2=None):
        self._connect_to_rabbit()
        self._channel = self._connection.channel()
        self._data_queue = data_queue
        self._channel.queue_declare(queue=data_queue)

        self._sink_exchange = sink_exchange
        self._channel.exchange_declare(exchange=sink_exchange, exchange_type='fanout')

        self._filter_operation = filter_operation
        self._filter_key = filter_key
        self._filter_parameter = filter_parameter
        self._filter_key_1 = filter_key_1
        self._filter_key_2 = filter_key_2
        self._A_R_F = []

    def _apply_filtering(self, data):
        return_value = False
        if self._filter_operation == "greater":
            return_value = data[self._filter_key] > self._filter_parameter
        elif self._filter_operation == "equal_fields":
            return_value = data[self._filter_key_1] == data[self._filter_key_2]
        elif self._filter_operation == "multi_key_special_filter":
            dict_value = list(data.values())[1]
            return_value = len(dict_value) == 1 and dict_value[list(dict_value.keys())[0]] >= self._filter_parameter
        return return_value

    def _send_flush_notification(self):
        self._channel.basic_publish(
            exchange=self._sink_exchange,
            routing_key='',
            body=bytes(json.dumps({'type': 'flush'}), encoding='utf-8')
        )

    def _process_data_chunk(self, data_chunk):
        filtered_data = [data for data in data_chunk if self._apply_filtering(data)]

        self._A_R_F.append(filtered_data)
        logging.info("Len of filtered data: {}".format(filtered_data))
        self._channel.basic_publish(
            exchange=self._sink_exchange,
            routing_key='',
            body=bytes(json.dumps({'type': 'data', 'data': filtered_data}), encoding='utf-8')
        )

    def _process_data(self, ch, method, properties, body):
        logging.info("Data arrived to filter.")
        data_chunk = json.loads(body.decode('utf-8'))
        if data_chunk['type'] == 'data':
            self._process_data_chunk(data_chunk['data'])
        else:
            self._send_flush_notification()
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        logging.info("Starting to listen for data to apply filtering.")
        self._channel.basic_consume(queue=self._data_queue, on_message_callback=self._process_data)
        self._channel.start_consuming()
