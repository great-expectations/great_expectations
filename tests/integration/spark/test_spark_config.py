from great_expectations.datasource import SparkDFDatasource
from great_expectations.execution_engine import SparkDFExecutionEngine


def test_spark_config_execution_engine(spark_session):
    # The below-commented assertions are not true for all cases,
    # because other parameters may have changed the global spark configuration.
    # Without specifying any spark_config values we get defaults.

    execution_engine: SparkDFExecutionEngine = SparkDFExecutionEngine(
        spark_config={
            "spark.app.name": "great_expectations-ee-config",
            "spark.sql.catalogImplementation": "hive",
            "spark.executor.memory": "512m",
        }
    )

    # Test that our values were set
    conf = execution_engine.spark.sparkContext.getConf().getAll()
    # assert ("spark.app.name", "great_expectations-ee-config") in conf
    assert (
        "spark.app.name",
        "default_great_expectations_spark_dataframe_execution_engine",
        # "default_great_expectations_spark_dataframe_datasource",
    ) in conf
    assert ("spark.sql.catalogImplementation", "hive") in conf
    # assert ("spark.executor.memory", "512m") in conf
    assert ("spark.executor.memory", "450m") in conf


def test_spark_config_datasource(spark_session_v012):
    # The below-commented assertions are not true for all cases,
    # because other parameters may have changed the global spark configuration.
    # Without specifying any spark_config values we get defaults.

    source = SparkDFDatasource(
        spark_config={
            "spark.app.name": "great_expectations-ds-config",
            "spark.sql.catalogImplementation": "hive",
            "spark.executor.memory": "768m",
        }
    )

    # Test that our values were set
    conf = source.spark.sparkContext.getConf().getAll()
    # assert ("spark.app.name", "great_expectations-ds-config") in conf
    assert (
        "spark.app.name",
        # "default_great_expectations_spark_dataframe_datasource",
        "default_great_expectations_spark_dataframe_execution_engine",
    ) in conf
    assert ("spark.sql.catalogImplementation", "hive") in conf
    # assert ("spark.executor.memory", "768m") in conf
    assert ("spark.executor.memory", "450m") in conf
