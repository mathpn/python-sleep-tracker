"""
Basic logging setup.
"""

import logging
import os


def setup_logging(log_level: int = logging.INFO):
    if 'DEBUG' in os.environ:
        log_level = logging.DEBUG

    if log_level == logging.DEBUG:
        format_str = '[%(asctime)s - %(levelname)s] [%(module)s.%(funcName)s]: %(message)s'
    else:
        format_str = '[%(asctime)s - %(levelname)s]: %(message)s'
    logging.basicConfig(format=format_str)
    logger = logging.getLogger()
    logger.setLevel(log_level)
    return logger

LOG = setup_logging()
