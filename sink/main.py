import logging
import os
from multiprocessing import Process, Barrier

from sink_direct_queue import SinkDirectQueue
from sink_fanout_exchange import SinkFanoutExchange


def parse_config_params():
    params = {
        'final_results_queue': os.environ['FINAL_RESULTS_QUEUE'],
        'sorted_cities_queue': os.environ['SORTED_CITIES_QUEUE'],
        'weekday_count_queue': os.environ['WEEKDAY_COUNT_QUEUE'],
        'users_fifty_reviews_exchange': os.environ['USERS_FIFTY_REVIEWS_EXCHANGE'],
        'five_stars_exchange': os.environ['FIVE_STARS_EXCHANGE'],
        'exchange_same_text_and_five_reviews': os.environ['EXCHANGE_SAME_TEXT_AND_FIVE_REVIEWS']
    }
    return params


def launch_sink_direct_queue(queue_name, push_result_queue, metric_name, metrics_barrier):
    sink = SinkDirectQueue(queue_name, push_result_queue, metric_name, metrics_barrier)
    sink.start()


def launch_sink_fanout_exchange(exchange_name, push_result_queue, metric_name, metrics_barrier):
    sink = SinkFanoutExchange(exchange_name, push_result_queue, metric_name, metrics_barrier)
    sink.start()


def main():
    initialize_log()
    logging.info("Starting sink node. Waiting for metrics.")
    config_params = parse_config_params()
    metrics_barrier = Barrier(5)
    sorted_cities_process = Process(target=launch_sink_direct_queue,
                                    args=(config_params['sorted_cities_queue'], config_params['final_results_queue'],
                                          'top_10_cities_by_review_quantity', metrics_barrier))
    sorted_cities_process.start()

    weekday_count_process = Process(target=launch_sink_direct_queue,
                                    args=(config_params['weekday_count_queue'], config_params['final_results_queue'],
                                          'weekday_count_for_reviews', metrics_barrier))

    weekday_count_process.start()

    user_fifty_reviews_process = Process(target=launch_sink_fanout_exchange,
                                         args=(config_params['users_fifty_reviews_exchange'],
                                               config_params['final_results_queue'],
                                               'users_with_fifty_or_more_reviews', metrics_barrier))
    user_fifty_reviews_process.start()

    user_fifty_reviews_5_stars_process = Process(target=launch_sink_fanout_exchange,
                                                 args=(config_params['five_stars_exchange'],
                                                       config_params['final_results_queue'],
                                                       'users_with_fifty_or_more_reviews_and_only_five_stars',
                                                       metrics_barrier))
    user_fifty_reviews_5_stars_process.start()

    user_five_reviews_and_same_text = Process(target=launch_sink_fanout_exchange,
                                              args=(config_params['exchange_same_text_and_five_reviews'],
                                                    config_params['final_results_queue'],
                                                    'users_with_five_or_more_reviews_and_always_same_text',
                                                    metrics_barrier))
    user_five_reviews_and_same_text.start()


def initialize_log():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


print(__name__)

if __name__ == "__main__":
    main()
