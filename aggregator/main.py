import os
import logging

from aggregator import Aggregator

def parse_config_params():
    params = {
        'reviews_by_day_queue': os.environ['REVIEWS_BY_DAY_QUEUE'],
    }
    return params

def main():
    initialize_log()
    logging.info("Starting aggregator")
    config_params = parse_config_params()
    reader = Aggregator(config_params['reviews_by_day_queue'])
    reader.start()


def initialize_log():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

print(__name__)

if __name__ == "__main__":
    main()
