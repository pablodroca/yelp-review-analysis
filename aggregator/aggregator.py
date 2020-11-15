import json
import pika
import logging
import datetime


class Aggregator:
    counter = {}

    def __init__(self, reviews_by_day_queue):
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq')
        )
        self._reviews_by_day_channel = self._connection.channel()
        self._reviews_by_day_queue = reviews_by_day_queue
        self._reviews_by_day_channel.queue_declare(queue=reviews_by_day_queue, durable=True)
        self._counter = {}

    @staticmethod
    def _process_reviews(reviews):
        for review in reviews:
            weekday = datetime.datetime.strptime('January 11, 2010', '%B %d, %Y').strftime('%A')

    @staticmethod
    def _process_reviews(ch, method, properties, body):
        reviews = json.loads(body.decode('utf-8'))
        if reviews['type'] == 'reviews':
            Aggregator._process_reviews(reviews['data'])
        else:
            logging.info('Finishing processing. Results: {}'.format(Aggregator.counter))


    def start(self):
        self._reviews_by_day_channel.basic_consume(queue=self._reviews_by_day_queue,
                                                   on_message_callback=Aggregator._process_reviews, auto_ack=True)
        self._reviews_by_day_channel.start_consuming()
