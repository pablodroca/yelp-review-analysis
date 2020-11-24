import datetime
import json
import logging
from time import sleep

import pika
import hashlib
from pika.exceptions import AMQPConnectionError


class ReviewController:
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
            except OSError:
                sleep(2)
                logging.info("Retrying connection to rabbit...")

    def __init__(self, review_queue, weekday_queue, weekday_aggregators_quantity, reviews_message_size,
                 exchange_requests, funny_reviews_queue, funny_reviews_joiners_quantity,
                 user_id_queue, user_id_aggregators_quantity,
                 five_stars_user_id_queue, five_stars_user_id_aggregators_quantity,
                 text_hash_queue, text_hash_aggregators_quantity):
        self._connect_to_rabbit()
        self._channel = self._connection.channel()
        self._channel.exchange_declare(exchange=exchange_requests, exchange_type='direct')
        self._review_queue_name = review_queue
        self._channel.queue_declare(queue=review_queue)
        self._channel.queue_bind(exchange=exchange_requests, queue=review_queue, routing_key=review_queue)

        self._weekday_queue = weekday_queue
        self._channel.queue_declare(queue=weekday_queue)

        self._funny_reviews_queue = funny_reviews_queue
        self._channel.queue_declare(queue=funny_reviews_queue)

        self._user_id_queue = user_id_queue
        self._channel.queue_declare(queue=user_id_queue)

        self._text_hash_queue = text_hash_queue
        self._channel.queue_declare(queue=text_hash_queue)

        self._five_stars_user_id_queue = five_stars_user_id_queue
        self._five_stars_user_id_aggregators_quantity = five_stars_user_id_aggregators_quantity

        self._weekday_aggregators_quantity = weekday_aggregators_quantity
        self._funny_reviews_joiner_quantity = funny_reviews_joiners_quantity
        self._user_id_aggregators_quantity = user_id_aggregators_quantity
        self._text_hash_aggregators_quantity = text_hash_aggregators_quantity
        self._reviews_message_size = reviews_message_size
        self._current_reviews = []
        self._total_reviews = 0

    @staticmethod
    def _data_bytes(message_type, data=None):
        return bytes(json.dumps({'type': message_type, 'data': data}), encoding='utf-8')

    def _publish(self, routing_key, body):
        self._channel.basic_publish(
            exchange='',
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(delivery_mode=2)
        )

    def _deliver_reviews(self, reviews):
        self._send_weekday_to_aggregators(reviews)
        self._send_funny_reviews_to_joiners(reviews)
        self._send_id_to_aggregators(reviews)
        self._send_id_five_stars_to_aggregators(reviews)
        self._send_hash_to_aggregators(reviews)

    def _flush_data(self):
        self._deliver_reviews(self._current_reviews)

        self._current_reviews = []

        data_bytes = self._data_bytes('flush')

        for _ in range(self._weekday_aggregators_quantity):
            self._publish(self._weekday_queue, data_bytes)

        for _ in range(self._funny_reviews_joiner_quantity):
            self._publish(self._funny_reviews_queue, data_bytes)

        for _ in range(self._user_id_aggregators_quantity):
            self._publish(self._user_id_queue, data_bytes)

        for _ in range(self._five_stars_user_id_aggregators_quantity):
            self._publish(self._five_stars_user_id_queue, data_bytes)

        for _ in range(self._text_hash_aggregators_quantity):
            self._publish(self._text_hash_queue, data_bytes)

        logging.info("Finishing processing stream data.")

    def _send_weekday_to_aggregators(self, reviews):
        data_bytes = self._data_bytes(
            'data', [{'weekday': datetime.datetime.strptime(review['date'], '%Y-%m-%d %H:%M:%S').strftime('%A')}
                     for review in reviews])
        self._publish(self._weekday_queue, data_bytes)

    def _send_funny_reviews_to_joiners(self, reviews):
        filtered_data = [
            {'business_id': review['business_id'],
             'review_id': review['review_id']}
            for review in reviews if review['funny'] > 0
        ]
        data_bytes = self._data_bytes('data', filtered_data)
        self._publish(self._funny_reviews_queue, data_bytes)

    def _send_id_to_aggregators(self, reviews):
        data_bytes = self._data_bytes('data', [{'user_id': review['user_id']} for review in reviews])
        self._publish(self._user_id_queue, data_bytes)

    def _send_id_five_stars_to_aggregators(self, reviews):
        data_bytes = self._data_bytes(
            'data', [{'user_id': review['user_id']} for review in reviews if int(review['stars']) == 5]
        )
        self._publish(self._five_stars_user_id_queue, data_bytes)

    def _send_hash_to_aggregators(self, reviews):
        data_bytes = self._data_bytes(
            'data', [{'user_id': review['user_id'], 'text_hash': hashlib.md5(review['text'].encode('utf-8')).hexdigest()} for review in reviews]
        )
        self._publish(self._text_hash_queue, data_bytes)

    def _process_data_chunk(self, received_reviews):
        if len(self._current_reviews) + len(received_reviews) >= self._reviews_message_size:
            total_reviews_to_append = self._reviews_message_size - len(self._current_reviews)
            propagation_reviews = self._current_reviews + received_reviews[:total_reviews_to_append]
            self._current_reviews = received_reviews[total_reviews_to_append:]
            self._deliver_reviews(propagation_reviews)
        else:
            self._current_reviews += received_reviews

    def _process_data(self, ch, method, properties, body):
        received_reviews = json.loads(body.decode('utf-8'))

        if received_reviews['type'] == 'data':
            self._process_data_chunk(received_reviews['data'])
            self._total_reviews += len(received_reviews['data'])
        else:
            self._flush_data()
            logging.info("Total reviews processed in this stream: {}".format(self._total_reviews))
            self._total_reviews = 0

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self._channel.basic_consume(queue=self._review_queue_name,
                                    on_message_callback=self._process_data)
        self._channel.start_consuming()
