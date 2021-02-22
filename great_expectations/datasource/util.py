import logging

logger = logging.getLogger(__name__)

try:
    from pyspark import SparkContext
    from pyspark.sql import SparkSession

    from great_expectations.datasource.sparkdf_datasource import SparkDFDatasource
except ImportError:
    SparkContext = None
    SparkSession = None
    SparkDFDatasource = None
    # TODO: review logging more detail here
    logger.debug(
        "Unable to load pyspark; install optional spark dependency for support."
    )
