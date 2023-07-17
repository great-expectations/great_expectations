import hashlib
import math
import os
from itertools import product

import pytest
from moto import mock_glue

from great_expectations.compatibility import sqlalchemy
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource import (
    Datasource,
    PandasDatasource,
    SparkDFDatasource,
)
from great_expectations.execution_engine.sparkdf_execution_engine import (
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.self_check.util import get_sqlite_connection_url


def create_partitions_for_table(
    glue_client, database_name: str, table_name: str, partitions: dict
):
    """
    This function is used to create partitions for a table in the Glue Data Catalog. It
    will create one partition per combination of partition values. Example: if we define
    the partitions {'year': [21, 22], 'month': [1,2]}, this function will create 4 partitions
    in the table:
        1. {'year': 21, 'month': 1}
        2. {'year': 21, 'month': 2}
        3. {'year': 22, 'month': 1}
        4. {'year': 22, 'month': 2}

    It is useful to test if the AWS Glue Data Connector can get the table partitions from
    the catalog and create one batch identifier per combination of partitions. The Glue connector
    will create batch ids based on the table partitions, like: {year=21, month=1}
    and {year=22, month=2}.
    """
    partition_values = list(product(*partitions.values()))
    partition_path = "={}/".join(partitions.keys()) + "={}/"

    for value in partition_values:
        path = partition_path.format(*value)
        glue_client.create_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionInput={
                "Values": list(value),
                "StorageDescriptor": {"Location": path},
            },
        )


@pytest.fixture(scope="module")
def test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine(
    titanic_spark_db,
):
    return SparkDFExecutionEngine(
        name="test_spark_execution_engine",
        force_reuse_spark_context=True,
    )


@pytest.fixture
def glue_titanic_catalog():
    try:
        import boto3
    except ImportError:
        raise ValueError(
            "AWS Glue Data Catalog Data Connector tests are requested, but boto3 is not installed"
        )

    os.environ[
        "AWS_DEFAULT_REGION"
    ] = "testing"  # required when connecting to the glue client, even when mocked

    with mock_glue():
        client = boto3.client("glue")
        database_name = "db_test"

        # Create Database
        client.create_database(DatabaseInput={"Name": database_name})

        # Create Table with Partitions
        client.create_table(
            DatabaseName=database_name,
            TableInput={
                "Name": "tb_titanic_with_partitions",
                "PartitionKeys": [
                    {
                        "Name": "PClass",
                        "Type": "string",
                    },
                    {
                        "Name": "SexCode",
                        "Type": "string",
                    },
                ],
            },
        )
        create_partitions_for_table(
            glue_client=client,
            database_name=database_name,
            table_name="tb_titanic_with_partitions",
            partitions={"PClass": ["1st", "2nd", "3rd"], "SexCode": ["0", "1"]},
        )

        # Create Table without Partitions
        client.create_table(
            DatabaseName=database_name,
            TableInput={
                "Name": "tb_titanic_without_partitions",
                "PartitionKeys": [],
            },
        )
        yield client


@pytest.fixture(scope="module")
def basic_pandas_datasource():
    return PandasDatasource("basic_pandas_datasource")


@pytest.fixture(scope="module")
def basic_sparkdf_datasource(test_backends):
    if "SparkDFDataset" not in test_backends:
        pytest.skip("Spark has not been enabled, so this test must be skipped.")
    return SparkDFDatasource("basic_sparkdf_datasource")


@pytest.fixture
def mysql_sqlalchemy_datasource(mysql_engine):
    return Datasource("mysql_sqlalchemy_datasource", engine=mysql_engine)


@pytest.fixture
def test_cases_for_sql_data_connector_sqlite_connection_url(sa):
    if sa is None:
        raise ValueError("SQL Database tests require sqlalchemy to be installed.")

    db_file_path: str = file_relative_path(
        __file__,
        os.path.join(  # noqa: PTH118
            "..", "test_sets", "test_cases_for_sql_data_connector.db"
        ),
    )

    return get_sqlite_connection_url(db_file_path)


@pytest.fixture
def test_cases_for_sql_data_connector_sqlite_execution_engine(
    sa, test_cases_for_sql_data_connector_sqlite_connection_url
):
    if sa is None:
        raise ValueError("SQL Database tests require sqlalchemy to be installed.")

    engine: sa.engine.Engine = sa.create_engine(
        test_cases_for_sql_data_connector_sqlite_connection_url,
        poolclass=sqlalchemy.StaticPool,
    )
    raw_connection = engine.raw_connection()
    raw_connection.create_function("sqrt", 1, lambda x: math.sqrt(x))
    raw_connection.create_function(
        "md5", 2, lambda x, d: hashlib.md5(str(x).encode("utf-8")).hexdigest()[-1 * d :]
    )

    conn: sa.engine.Connection = engine.connect()  # noqa: F841

    # Build a SqlAlchemyDataset using that database
    return SqlAlchemyExecutionEngine(
        name="test_sql_execution_engine",
        engine=engine,
    )
