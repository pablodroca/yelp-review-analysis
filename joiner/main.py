import os
import logging

from joiner import Joiner


def parse_config_params():
    params = {
        'source_queue': os.environ['SOURCE_QUEUE'],
        'reducer_queue': os.environ['REDUCER_QUEUE'],
        'file_path_to_join': os.environ['FILE_PATH_TO_JOIN']
    }
    return params


def main():
    initialize_log()
    logging.info("Starting reducer.")
    config_params = parse_config_params()
    joiner = Joiner(config_params['aggregated_data_queue'], config_params['sink_queue'],
                      config_params['aggregators_quantity'])
    joiner.start()


def initialize_log():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


if __name__ == "__main__":
    main()
