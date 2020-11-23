import os
import logging

from aggregator import Aggregator

def parse_config_params():
    params = {
        'source_queue': os.environ['SOURCE_QUEUE'],
        'reducer_queue': os.environ['REDUCER_QUEUE'],
        'key': os.environ['KEY']
    }
    return params

def main():
    initialize_log()
    logging.info("Starting aggregator")
    config_params = parse_config_params()
    aggregator = Aggregator(config_params['source_queue'], config_params['reducer_queue'], config_params['key'])
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
