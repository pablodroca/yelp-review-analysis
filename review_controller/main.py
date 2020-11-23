import os
import logging

from review_controller import ReviewController


def parse_config_params():
    params = {
        'reviews_queue': os.environ['REVIEWS_QUEUE'],
        'weekday_queue': os.environ['WEEKDAY_QUEUE'],
        'weekday_aggregators_quantity': int(os.environ['WEEKDAY_AGGREGATORS_QUANTITY']),
        'reviews_message_size': int(os.environ['REVIEWS_MESSAGE_SIZE']),
        'exchange_incoming_reviews': os.environ['EXCHANGE_INCOMING_REVIEWS'],
        'funny_reviews_queue': os.environ['FUNNY_REVIEWS_QUEUE'],
        'funny_reviews_joiners_quantity': int(os.environ['FUNNY_REVIEWS_JOINERS_QUANTITY']),
        'user_id_queue': os.environ['USER_ID_QUEUE'],
        'user_id_aggregators_quantity': int(os.environ['USER_ID_AGGREGATORS_QUANTITY']),
        'five_stars_user_id_queue': os.environ['FIVE_STARS_USER_ID_QUEUE'],
        'five_stars_user_id_aggregators_quantity': int(os.environ['FIVE_STARS_USER_ID_AGGREGATORS_QUANTITY']),
        'text_hash_queue': os.environ['TEXT_HASH_QUEUE'],
        'text_hash_aggregators_quantity': int(os.environ['TEXT_HASH_AGGREGATORS_QUANTITY'])
    }
    return params


def main():
    initialize_log()
    logging.info("Starting aggregator")
    config_params = parse_config_params()
    review_controller = ReviewController(config_params['reviews_queue'], config_params['weekday_queue'],
                                         config_params['weekday_aggregators_quantity'],
                                         config_params['reviews_message_size'],
                                         config_params['exchange_incoming_reviews'],
                                         config_params['funny_reviews_queue'],
                                         config_params['funny_reviews_joiners_quantity'],
                                         config_params['user_id_queue'],
                                         config_params['user_id_aggregators_quantity'],
                                         config_params['five_stars_user_id_queue'],
                                         config_params['five_stars_user_id_aggregators_quantity'],
                                         config_params['text_hash_queue'],
                                         config_params['text_hash_aggregators_quantity'])
    review_controller.start()


def initialize_log():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


print(__name__)

if __name__ == "__main__":
    main()
