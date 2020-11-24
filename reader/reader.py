import json
import pika
import logging

class Reader:
    def __init__(self, path, message_size, data_routing_key, exchange_requests, connection):
        self._path = path
        self._message_size = message_size
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self._channel = self._connection.channel()
        self._data_routing_key = data_routing_key
        self._channel.exchange_declare(exchange=exchange_requests, exchange_type='direct')
        self._exchange_requests = exchange_requests
        self._total_registers_sent = 0

    def _send_registers(self, registers):
        data_bytes = bytes(json.dumps(
            {'type': 'data',
             'data': registers
             }
        ), encoding='utf-8')

        self._channel.basic_publish(
            exchange=self._exchange_requests,
            routing_key=self._data_routing_key,
            body=data_bytes,
            properties=pika.BasicProperties(delivery_mode=2)
        )

        self._total_registers_sent += len(registers)

    def _send_flush_notification(self):
        data_bytes = bytes(json.dumps({'type': 'flush'}), encoding='utf-8')
        self._channel.basic_publish(
            exchange=self._exchange_requests,
            routing_key=self._data_routing_key,
            body=data_bytes,
            properties=pika.BasicProperties(delivery_mode=2)
        )

        logging.info("Finishing sending data to routing key: {}. Total registers sent: {}"
                     .format(self._data_routing_key, self._total_registers_sent))

    def start(self):
        logging.info("Starting to send data from file: {}.".format(self._path))
        with open(self._path, 'r') as data_file:
            current_register = data_file.readline().rstrip()
            current_registers = []
            while len(current_register) > 0:
                current_registers.append(json.loads(current_register))
                if len(current_registers) == self._message_size:
                    self._send_registers(current_registers)
                    current_registers = []

                current_register = data_file.readline().rstrip()

            if len(current_registers) > 0:
                self._send_registers(current_registers)

        self._send_flush_notification()