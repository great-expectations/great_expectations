import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

try:
    import pyspark
    from pyspark import SparkContext
    from pyspark.sql import SparkSession

    from great_expectations.datasource import SparkDFDatasource
    from great_expectations.execution_engine import SparkDFExecutionEngine
except ImportError:
    pyspark = None
    SparkContext = None
    SparkSession = None
    SparkDFDatasource = None
    SparkDFExecutionEngine = None
    # TODO: review logging more detail here
    logger.debug(
        "Unable to load pyspark; install optional spark dependency if you will be working with Spark dataframes."
    )


def test_spark_config_datasource(spark_session_v012):
    name: str = "great_expectations-ds-config"
    spark_config: Dict[str, str] = {
        "spark.app.name": name,
        "spark.sql.catalogImplementation": "hive",
        "spark.executor.memory": "768m",
        # "spark.driver.allowMultipleContexts": "true",  # This directive does not appear to have any effect.
    }
    source: SparkDFDatasource = SparkDFDatasource(spark_config=spark_config)
    spark_session: SparkSession = source.spark
    # noinspection PyProtectedMember
    sc_stopped: bool = spark_session.sparkContext._jsc.sc().isStopped()
    assert not sc_stopped

    # Test that our values were set
    conf: List[tuple] = source.spark.sparkContext.getConf().getAll()
    assert ("spark.app.name", name) in conf
    assert ("spark.sql.catalogImplementation", "hive") in conf
    assert ("spark.executor.memory", "768m") in conf


def test_spark_config_execution_engine(spark_session):
    name: str = "great_expectations-ee-config"
    spark_config: Dict[str, str] = {
        "spark.app.name": name,
        "spark.sql.catalogImplementation": "hive",
        "spark.executor.memory": "512m",
        # "spark.driver.allowMultipleContexts": "true",  # This directive does not appear to have any effect.
    }
    execution_engine: SparkDFExecutionEngine = SparkDFExecutionEngine(
        spark_config=spark_config
    )
    spark_session: SparkSession = execution_engine.spark
    # noinspection PyProtectedMember
    sc_stopped: bool = spark_session.sparkContext._jsc.sc().isStopped()
    assert not sc_stopped

    # Test that our values were set
    conf: List[tuple] = execution_engine.spark.sparkContext.getConf().getAll()
    assert ("spark.app.name", "great_expectations-ee-config") in conf
    assert ("spark.sql.catalogImplementation", "hive") in conf
    assert ("spark.executor.memory", "512m") in conf
