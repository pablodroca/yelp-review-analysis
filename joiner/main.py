import os
import logging

from multiprocessing import Manager, Semaphore, Process

from data_receiver import DataReceiver
from joiner import Joiner


def parse_config_params():
    params = {
        'critical_data_exchange': os.environ['CRITICAL_DATA_EXCHANGE'],
        'data_to_join_queue': os.environ['DATA_TO_JOIN_QUEUE'],
        'join_key': os.environ['JOIN_KEY'],
        'output_queue': os.environ['OUTPUT_QUEUE'],
        'flush_messages_quantity': int(os.environ['FLUSH_MESSAGES_QUANTITY'])
    }
    return params


def launch_joiner(data_to_join_queue, filled_table, table_filled_semaphore, join_key,
                  output_queue, flush_messages_quantity):
    joiner = Joiner(data_to_join_queue, filled_table, table_filled_semaphore, join_key,
                    output_queue, flush_messages_quantity)
    joiner.start()


def main():
    initialize_log()
    logging.info("Starting joiner.")
    config_params = parse_config_params()
    table_to_fill = Manager().dict()
    table_filled_semaphore = Semaphore(0)
    data_receiver = DataReceiver(config_params['critical_data_exchange'], table_to_fill,
                                 table_filled_semaphore, config_params['join_key'])
    joiner_process = Process(target=launch_joiner, args=(config_params['data_to_join_queue'], table_to_fill,
                                                         table_filled_semaphore, config_params['join_key'],
                                                         config_params['output_queue'],
                                                         config_params['flush_messages_quantity'],))
    joiner_process.start()
    data_receiver.start()


def initialize_log():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


if __name__ == "__main__":
    main()
