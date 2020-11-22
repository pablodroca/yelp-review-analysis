import os
import logging

from filter import Filter


def parse_config_params():
    params = {
        'data_queue': os.environ['DATA_QUEUE'],
        'sink_exchange': os.environ['SINK_EXCHANGE'],
        'filter_operation': os.environ['FILTER_OPERATION'],
        'filter_key': os.environ['FILTER_KEY'],
        'filter_parameter': int(os.environ['FILTER_PARAMETER'])
    }
    return params


def main():
    initialize_log()
    config_params = parse_config_params()
    logging.info("Starting filter for: {}, operation: {} and parameter {}.".format(
        config_params['filter_key'], config_params['filter_operation'], config_params['filter_parameter']
    ))
    filter = Filter(config_params['data_queue'], config_params['sink_exchange'],
                    config_params['filter_operation'], config_params['filter_key'],
                    config_params['filter_parameter'])
    filter.start()


def initialize_log():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


if __name__ == "__main__":
    main()
