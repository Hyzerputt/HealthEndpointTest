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

from config import RABBITMQ_CONFIG



# TODO globals: consumer, producer+connect


def report_health():
    pass

def call_back():
    pass
    # TODO


if __name__ == "__main__":
    pass
    # TODO connect consumer
    # TODO initialize daemon thread for health status
    # TODO listen loop with retry