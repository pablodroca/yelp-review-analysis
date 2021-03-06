import os
import logging

from business_controller import BusinessController


def parse_config_params():
    params = {
        'business_queue': os.environ['BUSINESS_QUEUE'],
        'business_exchange': os.environ['BUSINESS_EXCHANGE'],
        'business_message_size': int(os.environ['BUSINESS_MESSAGE_SIZE']),
        'exchange_incoming_business': os.environ['EXCHANGE_INCOMING_BUSINESS']
    }
    return params


def main():
    initialize_log()
    logging.info("Starting aggregator")
    config_params = parse_config_params()
    business_controller = BusinessController(config_params['business_queue'], config_params['business_exchange'],
                                             config_params['business_message_size'], config_params['exchange_incoming_business'])
    business_controller.start()


def initialize_log():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


print(__name__)

if __name__ == "__main__":
    main()
