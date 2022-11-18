import logging


def init_logger(level: int = logging.WARNING):
    logging.basicConfig(level=level, format="%(levelname)s:%(name)s | %(message)s")
