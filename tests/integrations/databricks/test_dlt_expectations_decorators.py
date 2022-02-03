from typing import List

import pandas as pd
import pytest

import integrations.databricks.dlt_expectations as dlt_expectations
from great_expectations.core import ExpectationConfiguration

# TODO: Get this test working with spark dataframes
# from great_expectations.core.util import get_or_create_spark_application
#
# spark = get_or_create_spark_application(
#     spark_config={
#         # TODO: is this spark_config required?
#         "spark.sql.catalogImplementation": "hive",
#         "spark.executor.memory": "450m",
#         # "spark.driver.allowMultipleContexts": "true",  # This directive does not appear to have any effect.
#     }
# )
#
#
# @pytest.fixture
# def simple_spark_df(spark_session):
#     pandas_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
#     df = spark.createDataFrame(data=pandas_df)
#     return df
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)
from great_expectations.util import gen_directory_tree_str
from integrations.databricks import dlt_mock_library_injected


@pytest.fixture
def simple_pandas_df():
    pandas_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    return pandas_df


@pytest.fixture
def expect_column_values_to_not_be_null_config():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={
            "column": "col2",
        },
    )


@pytest.fixture
def expect_column_values_to_be_between_strict_config():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={
            "column": "col1",
            "min_value": 1,
            "max_value": 5,
            "strict_min": True,
            "strict_max": True,
            "result_format": "COMPLETE",
        },
        meta={"notes": "This is an expectation from GE config."},
    )


@pytest.fixture
def in_memory_ge_context_with_filesystem_stores():
    """
    Construct a DataContext using path passed as parameter
    Returns:
        In-memory GE context with filesystem stores using root_directory from parameter
    """

    def _construct_data_context(root_directory):
        data_context_config = DataContextConfig(
            store_backend_defaults=FilesystemStoreBackendDefaults(
                root_directory=root_directory
            ),
        )
        context = BaseDataContext(project_config=data_context_config)

        datasource_config = {
            "name": "example_datasource",
            "class_name": "Datasource",
            "module_name": "great_expectations.datasource",
            "execution_engine": {
                "module_name": "great_expectations.execution_engine",
                "class_name": "PandasExecutionEngine",
            },
            "data_connectors": {
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
                    "batch_identifiers": ["default_identifier_name"],
                },
            },
        }

        context.add_datasource(**datasource_config)

        return context

    return _construct_data_context


def test_dlt_expect_decorator(
    tmp_path,
    in_memory_ge_context_with_filesystem_stores,
    simple_pandas_df,
    expect_column_values_to_be_between_strict_config,
    expect_column_values_to_not_be_null_config,
):
    # Load data context
    d = tmp_path / "great_expectations"
    d.mkdir()
    data_context = in_memory_ge_context_with_filesystem_stores(d)

    # To set off test output
    print("\n\nSTART =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
    # print("Starting directory tree structure")
    # print(gen_directory_tree_str(str(d)))
    print("\n", "Beginning of pipeline df:\n", simple_pandas_df, "\n")

    # Our first "dlt" transformation from a GE expectation
    @dlt_expectations.expect(
        data_context=data_context,
        dlt_expectation_name="my_expect_column_values_to_be_between_expectation",
        ge_expectation_configuration=expect_column_values_to_be_between_strict_config,
        dlt_library=dlt_mock_library_injected,
    )
    def transformation_1(df):
        # Note that in DLT, the dataframe is retrieved in the function body
        # e.g. dlt.read("clickstream_raw")
        df += 1
        return df

    # Our second "dlt" transformation from a DLT expectation
    @dlt_expectations.expect(
        data_context=data_context,
        dlt_expectation_name="my_expect_column_values_to_not_be_null_expectation",
        # ge_expectation_configuration=expect_column_values_to_not_be_null_config,
        dlt_expectation_condition="col2 IS NOT NULL",
        dlt_library=dlt_mock_library_injected,
    )
    def transformation_2(df):
        df += 2
        return df

    df_1 = transformation_1(simple_pandas_df)
    print("\n", "df after first transformation:\n", df_1, "\n")
    df_2 = transformation_2(df_1)

    print("\n", "Resulting end of pipeline df:\n", df_2, "\n")

    print("GE Expectation Suites")
    expectation_suite_names: List[str] = data_context.list_expectation_suite_names()
    # print(expectation_suite_names)
    for expectation_suite_name in expectation_suite_names:
        suite = data_context.get_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
        print(expectation_suite_name)
        print(suite)

    print("Ending directory tree structure")
    print(gen_directory_tree_str(str(d)))

    # To set off test output
    print("END =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
