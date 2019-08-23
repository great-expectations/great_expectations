""" pytest fixtures that can be resued across tests. the filename needs to be conftest.py
"""
import logging
import os
import shutil

# make sure env variables are set correctly
import findspark  # this needs to be the first import
import pytest
from pyspark.sql import SparkSession

findspark.init()


def quiet_py4j():
    """ turn down spark logging for the test context """
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


def clean_spark_dir():
    """

    :return:
    """
    try:
        os.remove("./derby.log")
        shutil.rmtree("./metastore_db")
        shutil.rmtree("./spark-warehouse")
    except OSError:
        pass


def clean_spark_session(session: SparkSession):
    """

    :param session:
    :return:
    """
    tables = session.catalog.listTables("default")

    for table in tables:
        print(f"clear_tables() dropping table/view: {table.name}")
        session.sql(f"DROP TABLE IF EXISTS default.{table.name}")
        session.sql(f"DROP VIEW IF EXISTS default.{table.name}")
        session.sql(f"DROP VIEW IF EXISTS {table.name}")

    session.catalog.clearCache()


def clean_close(session):
    """

    :param session:
    :return:
    """
    clean_spark_session(session)
    clean_spark_dir()
    session.stop()


@pytest.fixture(scope="session")
def spark_session(request):
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object
    """
    clean_spark_dir()
    session = SparkSession.builder.appName("pytest-pyspark-local-testing"). \
        master("local[2]"). \
        config("spark.executor.memory", "6g"). \
        config("spark.driver.memory", "6g"). \
        config("spark.ui.showConsoleProgress", "false"). \
        config("spark.sql.shuffle.partitions", "2"). \
        config("spark.default.parallelism", "4"). \
        enableHiveSupport(). \
        getOrCreate()
    request.addfinalizer(lambda: clean_close(session))
    quiet_py4j()
    return session
