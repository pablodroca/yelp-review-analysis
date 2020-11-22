import os
import logging

from filter import Filter


def parse_config_params():
    params = {
        'data_queue': os.environ['DATA_QUEUE'],
        'sink_exchange': os.environ['SINK_EXCHANGE'],
        'filter_operation': os.environ['FILTER_OPERATION'],
        'filter_key': os.environ['FILTER_KEY'] if 'FILTER_KEY' in os.environ else None,
        'filter_parameter': int(os.environ['FILTER_PARAMETER'] if 'FILTER_PARAMETER' in os.environ else None),
        'filter_key_1': os.environ['FILTER_KEY_1'] if 'FILTER_KEY_1' in os.environ else None,
        'filter_key_2': os.environ['FILTER_KEY_2'] if 'FILTER_KEY_2' in os.environ else None
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
                    config_params['filter_parameter'], config_params['filter_key_1'],
                    config_params['filter_key_2'])
    filter.start()


def initialize_log():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


if __name__ == "__main__":
    main()
