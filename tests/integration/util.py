import logging
import os
from logging import config

dirname = os.path.dirname(__file__)


def get_logger_from_config_file():
    config.fileConfig(os.path.join(dirname, "logging.conf"))
    logger = logging.getLogger("docs")
    return logger
