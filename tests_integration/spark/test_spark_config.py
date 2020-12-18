import pytest

from great_expectations.datasource import SparkDFDatasource
from great_expectations.execution_engine import SparkDFExecutionEngine


def test_spark_config_datasource(test_backends):
    if "SparkDFDataset" not in test_backends:
        pytest.skip("Spark has not been enabled, so this test must be skipped.")

    # The below-commented assertion is not true for all cases,
    # because other parameters may have changed the global spark configuration.
    # source = SparkDFDatasource()
    # conf = source.spark.sparkContext.getConf().getAll()
    # # Without specifying any spark_config values we get defaults
    # assert ("spark.app.name", "pyspark-shell") in conf

    source = SparkDFDatasource(
        spark_config={
            "spark.app.name": "great_expectations-ds-config",
            "spark.sql.catalogImplementation": "hive",
            "spark.executor.memory": "768m",
        }
    )

    # Test that our values were set
    conf = source.spark.sparkContext.getConf().getAll()
    assert ("spark.app.name", "great_expectations-ds-config") in conf
    assert ("spark.sql.catalogImplementation", "hive") in conf
    assert ("spark.executor.memory", "768m") in conf


def test_spark_config_execution_engine(test_backends):
    if "SparkDFDataset" not in test_backends:
        pytest.skip("Spark has not been enabled, so this test must be skipped.")

    # The below-commented assertion is not true for all cases,
    # because other parameters may have changed the global spark configuration.
    # source = SparkDFExecutionEngine()
    # conf = source.spark.sparkContext.getConf().getAll()
    # # Without specifying any spark_config values we get defaults
    # assert ("spark.app.name", "pyspark-shell") in conf

    source = SparkDFExecutionEngine(
        spark_config={
            "spark.app.name": "great_expectations-ee-config",
            "spark.sql.catalogImplementation": "hive",
            "spark.executor.memory": "512m",
        }
    )

    # Test that our values were set
    conf = source.spark.sparkContext.getConf().getAll()
    assert ("spark.app.name", "great_expectations-ee-config") in conf
    assert ("spark.sql.catalogImplementation", "hive") in conf
    assert ("spark.executor.memory", "512m") in conf
