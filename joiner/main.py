import os
import logging

from data_receiver import DataReceiver


def parse_config_params():
    params = {
        'critical_data_exchange': os.environ['CRITICAL_DATA_EXCHANGE'],
        # 'data_flow_queue': os.environ['DATAFLOW_QUEUE']
    }
    return params


def main():
    initialize_log()
    logging.info("Starting joiner.")
    config_params = parse_config_params()
    data_receiver = DataReceiver(config_params['critical_data_exchange'])
    data_receiver.start()


def initialize_log():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


if __name__ == "__main__":
    main()
