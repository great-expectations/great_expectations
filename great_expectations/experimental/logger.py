import logging


def init_logger(level: int = logging.WARNING):
    logging.basicConfig(level=level)
