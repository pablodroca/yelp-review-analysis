import os
import logging

from reducer import Reducer


def parse_config_params():
    params = {
        'aggregated_data_queue': os.environ['AGGREGATED_DATA_QUEUE'],
        'sink_queue': os.environ['SINK_QUEUE'],
        'aggregators_quantity': int(os.environ['AGGREGATORS_QUANTITY']),
        'unflatten_key': os.environ['UNFLATTEN_KEY'],
        'unflatten_value_key': os.environ['UNFLATTEN_VALUE_KEY']
    }
    return params


def main():
    initialize_log()
    logging.info("Starting reducer.")
    config_params = parse_config_params()
    reducer = Reducer(config_params['aggregated_data_queue'], config_params['sink_queue'],
                      config_params['aggregators_quantity'], config_params['unflatten_key'],
                      config_params['unflatten_value_key'])
    reducer.start()


def initialize_log():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


if __name__ == "__main__":
    main()
