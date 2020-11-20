import os
import logging

from review_controller import ReviewController

def parse_config_params():
    params = {
        'reviews_queue': os.environ['REVIEWS_QUEUE'],
        'weekday_queue': os.environ['WEEKDAY_QUEUE'],
        'weekday_aggregators_quantity': int(os.environ['WEEKDAY_AGGREGATORS_QUANTITY']),
        'reviews_message_size': int(os.environ['REVIEWS_MESSAGE_SIZE']),
        'exchange_requests': os.environ['EXCHANGE_REQUESTS']
    }
    return params

def main():
    initialize_log()
    logging.info("Starting aggregator")
    config_params = parse_config_params()
    aggregator = ReviewController(config_params['reviews_queue'], config_params['weekday_queue'],
                                  config_params['weekday_aggregators_quantity'], config_params['reviews_message_size'],
                                  config_params['exchange_requests'])
    aggregator.start()


def initialize_log():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

print(__name__)

if __name__ == "__main__":
    main()
