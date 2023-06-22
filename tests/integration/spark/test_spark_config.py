import logging
from typing import Any, Dict, List

from packaging.version import Version
from packaging.version import parse as parse_version

from great_expectations.compatibility import pyspark

logger = logging.getLogger(__name__)

try:
    from great_expectations.datasource import SparkDFDatasource
    from great_expectations.execution_engine import SparkDFExecutionEngine
except ImportError:
    SparkDFDatasource = None
    SparkDFExecutionEngine = None
    # TODO: review logging more detail here
    logger.debug(
        "Unable to load pyspark; install optional spark dependency if you will be working with Spark dataframes."
    )


def test_current_pyspark_version_installed(spark_session):
    pyspark_version: Version = parse_version(pyspark.pyspark.__version__)
    # Spark versions less than 3.0 are not supported.
    assert pyspark_version.major >= 3, "Spark versions less than 3.0 are not supported."


def test_spark_config_datasource(spark_session_v012):
    name: str = "great_expectations-ds-config"
    spark_config: Dict[str, Any] = {
        "spark.app.name": name,
        "spark.sql.catalogImplementation": "hive",
        "spark.executor.memory": "768m",
        # "spark.driver.allowMultipleContexts": "true",  # This directive does not appear to have any effect.
    }
    source: SparkDFDatasource = SparkDFDatasource(spark_config=spark_config)
    spark_session: pyspark.SparkSession = source.spark
    # noinspection PyProtectedMember
    sc_stopped: bool = spark_session.sparkContext._jsc.sc().isStopped()
    assert not sc_stopped

    # Test that our values were set
    conf: List[tuple] = source.spark.sparkContext.getConf().getAll()
    assert ("spark.app.name", name) in conf
    assert ("spark.sql.catalogImplementation", "hive") in conf
    assert ("spark.executor.memory", "768m") in conf
    spark_session.sparkContext.stop()


def test_spark_config_execution_engine(spark_session):
    old_app_id = spark_session.sparkContext.applicationId
    new_spark_config: Dict[str, Any] = {
        "spark.app.name": "great_expectations-ee-config",
        "spark.sql.catalogImplementation": "hive",
        "spark.executor.memory": "512m",
        # "spark.driver.allowMultipleContexts": "true",  # This directive does not appear to have any effect.
    }
    execution_engine = SparkDFExecutionEngine(spark_config=new_spark_config)
    new_spark_session: pyspark.SparkSession = execution_engine.spark

    # noinspection PyProtectedMember
    sc_stopped: bool = new_spark_session.sparkContext._jsc.sc().isStopped()

    assert not sc_stopped

    current_spark_config: List[
        tuple
    ] = execution_engine.spark.sparkContext.getConf().getAll()
    assert old_app_id == execution_engine.spark.sparkContext.applicationId
    assert ("spark.sql.catalogImplementation", "hive") in current_spark_config
    # Confirm that "spark.app.name" was not changed upon "SparkDFExecutionEngine" instantiation (from original value).
    assert (
        "spark.app.name",
        "default_great_expectations_spark_application",
    ) in current_spark_config
    assert ("spark.executor.memory", "450m") in current_spark_config
    # spark context config values cannot be changed by the builder no matter what
    assert current_spark_config != new_spark_config
    new_spark_session.sparkContext.stop()
