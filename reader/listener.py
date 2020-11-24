import pika
import json
import logging


class Listener:
    def __init__(self, final_results_queue, total_metrics):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self._channel = self._connection.channel()
        self._channel.queue_declare(final_results_queue)
        self._final_results_queue = final_results_queue
        self._total_metrics = total_metrics
        self._already_received_metrics = 0

    def _process_data(self, ch, method, properties, body):
        data = json.loads(body.decode('utf-8'))
        if 'users_with_fifty_or_more_reviews' in data:
            logging.info('Amount user with fifty or more reviews: {}'.format(len(data['users_with_fifty_or_more_reviews'])))
        else:
            logging.info("Received metric: {}".format(data))
        self._already_received_metrics += 1
        ch.basic_ack(delivery_tag=method.delivery_tag)

        if self._already_received_metrics == self._total_metrics:
            self._connection.close()

    def start(self):
        logging.info("Starting to listen for output metrics.")
        self._channel.basic_consume(queue=self._final_results_queue, on_message_callback=self._process_data)
        self._channel.start_consuming()
