#! usr/bin/env python3.7
import logging
from loguru import logger


class InterceptHandler(logging.Handler):
    """
    Handler to route stdlib logs to loguru
    """

    def emit(self, record):
        # Retrieve context where the logging call occurred, this happens to be in the 6th frame upward
        logger_opt = logger.opt(depth=6, exception=record.exc_info)
        logger_opt.log(record.levelno, record.getMessage())


# Configuration for stdlib logger to route messages to loguru; must be run before other imports
LOG_LEVEL_INFO = 20
logging.basicConfig(handlers=[InterceptHandler()], level=LOG_LEVEL_INFO)

from collections import deque
from time import sleep
from pprint import pprint
import random
import sys
from datetime import datetime
import os
import subprocess
import threading

from health_endpoint_example.models import HealthReport, RabbitMQHealth
from health_endpoint_example.shared_services.rabbitmq import ConsumerConnector
from health_endpoint_example.config import RABBITMQ_CONFIG

logger.debug(f"Setting Globals")
response_time_list_length = 5
response_times = deque(response_time_list_length*[0], maxlen=response_time_list_length)
service_start_date = datetime.utcnow().isoformat('T')
health_report_interval = 2
rabbitmq_connection_failures = 0
last_message_processed_time = None




consumer = ConsumerConnector(**RABBITMQ_CONFIG, consumer_queue='health_test')

class HealthDaemon(threading.Thread):
    def __init__(self):
        with open('health_endpoint_example/health_report/health.json', 'wt') as f:
            pprint({"status": "Starting"}, stream=f)
        wd = os.getcwd()
        os.chdir('./health_endpoint_example/health_report')
        self.web_server = subprocess.Popen(['python', '-m', 'http.server', '8080'])
        os.chdir(wd)


        # todo initialize health.json

    def run(self) -> None:
        pass
        # todo report health

    def __enter__(self):
        pass
        # todo start thread as daemon
        #todo retrun self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
        # todo update health.json status:bad
        # todo ? stop webserv er subprocess
        #

def report_health_daemon():
    logger.debug(f"Starting Health Daemon")
    health = HealthReport()
    with threading.Lock():
        logger.debug(f"Initialize Health Report")
        with open('health_endpoint_example/health_report/health.json', 'wt') as health_file:
            pprint(health.dict(), stream=health_file)
            logger.success(f"Health Report Initialized")

    while True:
        logger.debug(f"Creating Health Report")


        health.status = "?" # TODO how to catch !ok
        health.status_date=datetime.utcnow().isoformat('T')
        rabbbitmq_health = RabbitMQHealth(is_connected=consumer.is_connected,
                                          connection_properties=str(consumer.connection_parameters))
        health.rabbit_mq = rabbbitmq_health
        health.recent_response_times = list(response_times)
        health.recent_response_times_average = sum(health.recent_response_times)/response_time_list_length
        with threading.Lock():
            logger.debug(f"Publishing Health Report")
            with open('health_endpoint_example/health_report/health.json', 'wt') as health_file:
                pprint(health.dict(), stream=health_file)
                logger.success(f"Health Report Published")
        for i in range(5):
            logger.debug(f"Sleeping report daemon {i}s")
            sleep(1)


def mock_listen(num_runs: int = 5, run_time_min: float = .1, run_time_max: float = 5.0) -> None:
    logger.info(f"Starting mock_listen: num_runs={num_runs}")
    for i in range(num_runs):
        logger.info(f"Mocking run {i+1}")
        start = datetime.now()
        sleep_duration=random.uniform(run_time_min, run_time_max)
        logger.info(f"Starting Sleep Mock: duration={sleep_duration}")
        sleep(sleep_duration)
        logger.info(f"Finished Sleep Mock")
        with threading.Lock():
            logger.info(f"Updating Response Time")
            duration = datetime.now() - start
            response_times.appendleft(duration.total_seconds())
            logger.info(f"Repsonse Time Updated: duration={duration}")





if __name__ == "__main__":
    health_worker = threading.Thread(target=report_health_daemon, daemon=True)
    health_worker.start()
    if len(sys.argv) > 1:
        mock_listen(num_runs=int(sys.argv[1]))
    else:
        mock_listen()
    #while True:
    #    consumer.connect()


def call_back(ch, method, properties, body) -> None:
    """
    Callback function to parse message type, and trigger an removal.
    :param ch: Copy of the channel used to acknowledge receipt
    :param method: Management keys for the delivered message e.g. delivery mode
    :param properties: Message properties
    :param body: Message body for a transfer message
    :return: None
    """
    pass
    # TODO