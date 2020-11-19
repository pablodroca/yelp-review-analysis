import os
import logging

from multiprocessing import Process

from reader import Reader


def parse_config_params():
    params = {
        'reviews_path': os.environ['REVIEWS_PATH'],
        'reviews_message_size': int(os.environ['REVIEWS_MESSAGE_SIZE']),
        'reviews_queue': os.environ['REVIEWS_QUEUE'],
        'business_path': os.environ['BUSINESS_PATH'],
        'business_message_size': int(os.environ['BUSINESS_MESSAGE_SIZE']),
        'business_queue': os.environ['BUSINESS_QUEUE'],
    }
    return params


def launch_business_reader(business_path, business_message_size, business_queue):
    reader = Reader(business_path, business_message_size, business_queue)
    reader.start()


def main():
    initialize_log()
    logging.info("Starting client.")
    config_params = parse_config_params()
    business_reader_process = Process(target=launch_business_reader, args=(
        config_params['business_path'], config_params['business_message_size'], config_params['business_queue']
    ))
    business_reader_process.start()
    reader = Reader(config_params['reviews_path'], config_params['reviews_message_size'],
                    config_params['reviews_queue'])
    reader.start()


def initialize_log():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


if __name__ == "__main__":
    main()
