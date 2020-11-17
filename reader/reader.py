import datetime
import json

import pika


class Reader:
    def __init__(self, reviews_path, reviews_message_size, reviews_by_day_queue,
                 reviews_by_day_aggregator_quantity):
        self._reviews_path = reviews_path
        self._reviews_message_size = reviews_message_size

        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq')
        )
        self._reviews_by_day_channel = self._connection.channel()
        self._reviews_by_day_queue = reviews_by_day_queue
        self._reviews_by_day_channel.queue_declare(queue=reviews_by_day_queue)

        self._reviews_by_day_aggregators_quantity = reviews_by_day_aggregator_quantity

    def _send_reviews(self, reviews):
        data_bytes = bytes(json.dumps(
            {'type': 'data',
             'data': [datetime.datetime.strptime(review['date'], '%Y-%m-%d %H:%M:%S').strftime('%A') for review in reviews]}
        ), encoding='utf-8')
        self._reviews_by_day_channel.basic_publish(
            exchange='',
            routing_key=self._reviews_by_day_queue,
            body=data_bytes,
            properties=pika.BasicProperties(delivery_mode=2)
        )

    def _send_flush_notification(self):
        for _ in range(self._reviews_by_day_aggregators_quantity):
            data_bytes = bytes(json.dumps({'type': 'flush'}), encoding='utf-8')
            self._reviews_by_day_channel.basic_publish(
                exchange='',
                routing_key=self._reviews_by_day_queue,
                body=data_bytes,
                properties=pika.BasicProperties(delivery_mode=2)
            )

    def start(self):
        with open(self._reviews_path, 'r') as reviews_file:
            current_review = reviews_file.readline().rstrip()
            current_reviews = []
            while len(current_review) > 0:
                current_reviews.append(json.loads(current_review))
                if len(current_reviews) == self._reviews_message_size:
                    self._send_reviews(current_reviews)
                    current_reviews = []

                current_review = reviews_file.readline().rstrip()

            if len(current_reviews) > 0:
                self._send_reviews(current_reviews)

        self._send_flush_notification()
        self._connection.close()
