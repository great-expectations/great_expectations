import logging
import os
from typing import Dict, List

import pytest
from packaging.version import parse as parse_version

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


@pytest.mark.parametrize("databricks_runtime", [False, True])
def test_spark_config_execution_engine(spark_session, databricks_runtime):
    # we observe different behaviour depending on Spark version:
    # https://spark.apache.org/docs/3.0.0/pyspark-migration-guide.html#upgrading-from-pyspark-24-to-30
    pyspark_version = parse_version(pyspark.__version__)

    # keep track of spark app id
    old_app_id = spark_session.sparkContext.applicationId

    if databricks_runtime:
        # simulate a databricks runtime environment by setting the databricks runtime version
        os.environ["DATABRICKS_RUNTIME_VERSION"] = "7.3"

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

    # Test that our values were set if not running in a Databricks runtime
    conf: List[tuple] = execution_engine.spark.sparkContext.getConf().getAll()
    if not databricks_runtime:
        # we can safely "restart" the SparkContext
        if pyspark_version.major >= 3:
            # spark context is restarted because the "spark.app.name" is not the one desired
            # this happens because from Spark 3, the SparkSession builder won't update config values
            assert old_app_id != execution_engine.spark.sparkContext.applicationId
        assert ("spark.app.name", "great_expectations-ee-config") in conf
        assert ("spark.sql.catalogImplementation", "hive") in conf
        assert ("spark.executor.memory", "512m") in conf
    else:
        # we should not "restart" the SparkContext
        if pyspark_version.major >= 3:
            assert old_app_id == execution_engine.spark.sparkContext.applicationId
            # spark context config values are not changed
            assert (
                "spark.app.name",
                "default_great_expectations_spark_application",
            ) in conf
        else:
            # spark context config values are changed by the builder no matter what
            assert ("spark.app.name", "great_expectations-ee-config") in conf
            assert ("spark.sql.catalogImplementation", "hive") in conf
            assert ("spark.executor.memory", "512m") in conf
