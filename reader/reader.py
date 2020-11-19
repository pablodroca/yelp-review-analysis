import json
import pika


class Reader:
    def __init__(self, path, message_size, queue_name):
        self._path = path
        self._message_size = message_size
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq')
        )
        self._channel = self._connection.channel()
        self._queue_name = queue_name
        self._channel.queue_declare(queue=queue_name)

    def _send_registers(self, registers):
        data_bytes = bytes(json.dumps(
            {'type': 'data',
             'data': registers
             }
        ), encoding='utf-8')

        self._channel.basic_publish(
            exchange='',
            routing_key=self._queue_name,
            body=data_bytes,
            properties=pika.BasicProperties(delivery_mode=2)
        )

    def _send_flush_notification(self):
        data_bytes = bytes(json.dumps({'type': 'flush'}), encoding='utf-8')
        self._channel.basic_publish(
            exchange='',
            routing_key=self._queue_name,
            body=data_bytes,
            properties=pika.BasicProperties(delivery_mode=2)
        )

    def start(self):
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
        self._connection.close()
