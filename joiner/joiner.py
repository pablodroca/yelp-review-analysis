import logging
import json
from time import sleep
import pika
from pika.exceptions import AMQPConnectionError


class Joiner:
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

    def __init__(self, data_to_join_queue, filled_table, table_filled_semaphore, join_key,
                 output_queue, flush_messages_quantity):
        self._connect_to_rabbit()
        self._channel = self._connection.channel()
        self._data_to_join_queue = data_to_join_queue
        self._channel.queue_declare(queue=data_to_join_queue)
        self._output_queue = output_queue
        self._channel.queue_declare(queue=output_queue)
        self._filled_table = filled_table
        self._table_filled_semaphore = table_filled_semaphore
        self._join_key = join_key
        self._consumer_tag = None
        self._flush_messages_quantity = flush_messages_quantity

    def _flush_data(self):
        for _ in range(self._flush_messages_quantity):
            self._channel.basic_publish(
                exchange='',
                routing_key=self._output_queue,
                body=bytes(json.dumps({'type': 'flush'}), encoding='utf-8'),
                properties=pika.BasicProperties(delivery_mode=2)
            )

    def _send_registers(self, registers):
        data_bytes = bytes(json.dumps({'type': 'data', 'data': registers}), encoding='utf-8')
        self._channel.basic_publish(
            exchange='',
            routing_key=self._output_queue,
            body=data_bytes,
            properties=pika.BasicProperties(delivery_mode=2)
        )

        if self._join_key == 'user_id':
            logging.info("Registers:".format(registers))

    def _process_data_chunk(self, registers):
        joined_registers = []
        for register in registers:
            joined_register = {}
            joining_key = register[self._join_key]
            if joining_key in self._filled_table:
                for key, value in self._filled_table[register[self._join_key]].items():
                    joined_register[key] = value
                for key, value in register.items():
                    joined_register[key] = value
                joined_registers.append(joined_register)

        self._send_registers(joined_registers)

    def _process_data(self, ch, method, properties, body):
        data_to_join = json.loads(body.decode('utf-8'))
        end_stream_message = False
        if data_to_join['type'] == 'data':
            self._process_data_chunk(data_to_join['data'])
        else:
            self._flush_data()
            logging.info("Finishing processing join data. Releasing semaphore.")
            end_stream_message = True

        ch.basic_ack(delivery_tag=method.delivery_tag)

        if end_stream_message:
            self._table_filled_semaphore.release()
            self._table_filled_semaphore.acquire()
            logging.info("Starting to listen for another stream.")

    def start(self):
        self._table_filled_semaphore.acquire()
        logging.info("Starting to listen for join data.")
        self._consumer_tag = self._channel.basic_consume(queue=self._data_to_join_queue, on_message_callback=self._process_data)
        self._channel.start_consuming()
