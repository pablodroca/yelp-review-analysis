import datetime
import json
import logging
import pika


class ReviewController:
    def __init__(self, review_queue, weekday_queue, weekday_aggregators_quantity, reviews_message_size,
                 exchange_requests):
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq')
        )
        self._review_queue_channel = self._connection.channel()
        self._review_queue_channel.exchange_declare(exchange=exchange_requests, exchange_type='direct')
        self._review_queue_name = review_queue
        self._review_queue_channel.queue_declare(queue=review_queue)
        self._review_queue_channel.queue_bind(exchange=exchange_requests, queue=review_queue, routing_key=review_queue)

        self._weekday_queue_channel = self._connection.channel()
        self._weekday_queue_name = weekday_queue
        self._weekday_queue_channel.queue_declare(queue=weekday_queue)

        self._weekday_aggregators_quantity = weekday_aggregators_quantity
        self._reviews_message_size = reviews_message_size
        self._current_reviews = []
        self._total_reviews = 0

    def _flush_data(self):
        self._send_weekday_to_aggregators(self._current_reviews)
        self._current_reviews = []

        for _ in range(self._weekday_aggregators_quantity):
            data_bytes = bytes(json.dumps({'type': 'flush'}), encoding='utf-8')
            self._weekday_queue_channel.basic_publish(
                exchange='',
                routing_key=self._weekday_queue_name,
                body=data_bytes,
                properties=pika.BasicProperties(delivery_mode=2)
            )

        logging.info("Finishing processing stream data.")

    def _send_weekday_to_aggregators(self, reviews):
        data_bytes = bytes(json.dumps(
            {'type': 'data',
             'data': [datetime.datetime.strptime(review['date'], '%Y-%m-%d %H:%M:%S').strftime('%A') for review in
                      reviews]}
        ), encoding='utf-8')
        self._weekday_queue_channel.basic_publish(
            exchange='',
            routing_key=self._weekday_queue_name,
            body=data_bytes,
            properties=pika.BasicProperties(delivery_mode=2)
        )

    def _process_data_chunk(self, received_reviews):
        if len(self._current_reviews) + len(received_reviews) >= self._reviews_message_size:
            total_reviews_to_append = self._reviews_message_size - len(self._current_reviews)
            propagation_reviews = self._current_reviews + received_reviews[:total_reviews_to_append]
            self._current_reviews = received_reviews[total_reviews_to_append:]
            self._send_weekday_to_aggregators(propagation_reviews)
        else:
            self._current_reviews += received_reviews

    def _process_data(self, ch, method, properties, body):
        received_reviews = json.loads(body.decode('utf-8'))

        if received_reviews['type'] == 'data':
            self._process_data_chunk(received_reviews['data'])
            self._total_reviews += len(received_reviews['data'])
        else:
            self._flush_data()
            logging.info("Total reviews: {}".format(self._total_reviews))

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self._review_queue_channel.basic_consume(queue=self._review_queue_name,
                                                 on_message_callback=self._process_data)
        self._review_queue_channel.start_consuming()
