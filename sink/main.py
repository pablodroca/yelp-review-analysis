import os
import logging
import pika
import json

from sink import SinkDirectQueue

from multiprocessing import Process, Queue


def parse_config_params():
    params = {
        'final_results_queue': os.environ['FINAL_RESULTS_QUEUE'],
        'sorted_cities_queue': os.environ['SORTED_CITIES_QUEUE'],
    }
    return params


def launch_sink_direct_queue(queue_name, push_result_queue, metric_name):
    sink = SinkDirectQueue(queue_name, push_result_queue, metric_name)
    sink.start()


def launch_sink_fanout_exchange(exchange_name, push_result_queue, metric_name):
    pass


def main():
    initialize_log()
    logging.info("Starting sink node. Waiting for metrics.")
    config_params = parse_config_params()
    sorted_cities_process = Process(target=launch_sink_direct_queue,
                                    args=(config_params['sorted_cities_queue'], config_params['final_results_queue'],
                                          'top_10_cities_by_review_quantity'))
    sorted_cities_process.start()


def initialize_log():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


print(__name__)

if __name__ == "__main__":
    main()
