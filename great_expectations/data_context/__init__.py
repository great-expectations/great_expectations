import logging

from .pandas_context import PandasCSVDataContext

logger = logging.getLogger(__name__)

try:
    from .sqlalchemy_context import SqlAlchemyDataContext
except ImportError:
    logger.info("Unable to load SqlAlchemy context; install optional sqlalchemy dependency for support")

try:
    from .spark_context import SparkCSVDataContext
    from .databricks_context import DatabricksTableContext
except ImportError:
    logger.info("Unable to load Spark contexts; install optional spark dependency for support")


def get_data_context(context_type, options, *args, **kwargs):
    """Return a data_context object which exposes options to list datasets and get a dataset from
    that context. This is a new API in Great Expectations 0.4, and is subject to rapid change.

    :param context_type: (string) one of "SqlAlchemy", "PandasCSV", "SparkCSV", or "DatabricksTable"
    :param options: options to be passed to the data context's connect method.
    :return: a new DataContext object
    """
    if context_type == "SqlAlchemy":
        return SqlAlchemyDataContext(options, *args, **kwargs)
    elif context_type == "PandasCSV":
        return PandasCSVDataContext(options, *args, **kwargs)
    elif context_type == "SparkCSV":
        return SparkCSVDataContext(options, *args, **kwargs)
    elif context_type == "DatabricksTable":
        return DatabricksTableContext(options, *args, **kwargs)
    else:
        raise ValueError("Unknown data context.")
