import os
import logging

from sorter import Sorter


def parse_config_params():
    params = {
        'data_queue': os.environ['DATA_QUEUE'],
        'sink_queue': os.environ['SINK_QUEUE'],
        'quantity_to_take': int(os.environ['QUANTITY_TO_TAKE'])
    }
    return params


def main():
    initialize_log()
    logging.info("Starting aggregator")
    config_params = parse_config_params()
    sorter = Sorter(config_params['data_queue'], config_params['sink_queue'], config_params['quantity_to_take'])
    sorter.start()


def initialize_log():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


print(__name__)

if __name__ == "__main__":
    main()
