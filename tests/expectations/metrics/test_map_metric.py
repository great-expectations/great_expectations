import pandas
import pandas as pd
import pytest

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
    IDDict,
)
from great_expectations.core.batch import Batch, BatchDefinition, BatchRequest
from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context import AbstractDataContext
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource import Datasource
from great_expectations.datasource.data_connector import ConfiguredAssetSqlDataConnector
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import (
    MetricPartialFunctionTypes,
)
from great_expectations.expectations.core import ExpectColumnValuesToBeInSet
from great_expectations.expectations.metrics import (
    ColumnMax,
    ColumnValuesNonNull,
    CompoundColumnsUnique,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    MapMetricProvider,
)
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator


@pytest.fixture
def pandas_animals_dataframe_for_unexpected_rows_and_index():
    return pd.DataFrame(
        {
            "pk_1": [0, 1, 2, 3, 4, 5],
            "pk_2": ["zero", "one", "two", "three", "four", "five"],
            "animals": [
                "cat",
                "fish",
                "dog",
                "giraffe",
                "lion",
                "zebra",
            ],
        }
    )


@pytest.fixture
def spark_dataframe_for_unexpected_rows_with_index(
    spark_session,
) -> "pyspark.sql.dataframe.DataFrame":  # noqa: F821
    df: pandas.DataFrame = pd.DataFrame(
        {
            "pk_1": [0, 1, 2, 3, 4, 5],
            "pk_2": ["zero", "one", "two", "three", "four", "five"],
            "animals": [
                "cat",
                "fish",
                "dog",
                "giraffe",
                "lion",
                "zebra",
            ],
        }
    )
    test_df: "pyspark.sql.dataframe.DataFrame" = spark_session.createDataFrame(
        data=df
    )  # noqa: F821
    return test_df


@pytest.fixture
def sqlite_table_for_unexpected_rows_with_index(
    test_backends,
) -> "sqlalchemy.engine.Engine":  # noqa: F821
    if "sqlite" in test_backends:
        try:
            import sqlalchemy as sa

            sqlite_path = file_relative_path(
                __file__, "../../test_sets/metrics_test.db"
            )
            sqlite_engine = sa.create_engine(f"sqlite:///{sqlite_path}")
            df = pd.DataFrame(
                {
                    "pk_1": [0, 1, 2, 3, 4, 5],
                    "pk_2": ["zero", "one", "two", "three", "four", "five"],
                    "animals": [
                        "cat",
                        "fish",
                        "dog",
                        "giraffe",
                        "lion",
                        "zebra",
                    ],
                }
            )
            # use try-except block to ensure we don't keep modifying the database
            # adapted from https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
            try:
                df.to_sql(
                    name="animal_names",
                    con=sqlite_engine,
                    index=False,
                    if_exists="fail",
                )
            except ValueError:
                pass

            return sqlite_engine
        except ImportError:
            sa = None
    else:
        pytest.skip("SqlAlchemy tests disabled; not testing views")


@pytest.fixture()
def expected_evr_without_unexpected_rows():
    return ExpectationValidationResult(
        success=False,
        expectation_config={
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "animals",
                "value_set": ["cat", "fish", "dog"],
            },
            "meta": {},
        },
        result={
            "element_count": 6,
            "missing_count": 0,
            "missing_percent": 0.0,
            "partial_unexpected_counts": [
                {"count": 1, "value": "giraffe"},
                {"count": 1, "value": "lion"},
                {"count": 1, "value": "zebra"},
            ],
            "partial_unexpected_index_list": [3, 4, 5],
            "partial_unexpected_list": ["giraffe", "lion", "zebra"],
            "unexpected_count": 3,
            "unexpected_index_list": [3, 4, 5],
            "unexpected_list": ["giraffe", "lion", "zebra"],
            "unexpected_percent": 50.0,
            "unexpected_percent_nonmissing": 50.0,
            "unexpected_percent_total": 50.0,
        },
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        meta={},
    )


def _expecation_configuration_to_validation_result_pandas(
    expectation_configuration: ExpectationConfiguration,
    dataframe: pd.DataFrame,
    context: AbstractDataContext,
) -> ExpectationValidationResult:
    """
    Helper method used by pandas tests in this suite. Takes in a ExpectationConfiguration and returns an EVR
    after building an ExecutionEngine, DataConnector and Validator.

    Args:
        expectation_configuration (ExpectationConfiguration): configuration that is being tested

    """
    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch_definition = BatchDefinition(
        datasource_name="pandas_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="my_asset",
        batch_identifiers=IDDict({}),
        batch_spec_passthrough=None,
    )
    batch = Batch(
        data=dataframe,
        batch_definition=batch_definition,
    )
    engine = PandasExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        data_context=context,
        batches=[
            batch,
        ],
    )
    result = expectation.validate(validator)
    return result


def _expecation_configuration_to_validation_result_sql(
    expectation_configuration: ExpectationConfiguration,
    context: AbstractDataContext,
) -> ExpectationValidationResult:
    """
    Helper method used by sql tests in this suite. Takes in a ExpectationConfiguration and returns an EVR
    after building an ExecutionEngine, DataConnector and Validator.

    Args:
        expectation_configuration (ExpectationConfiguration): configuration that is being tested

    """
    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    sqlite_path = file_relative_path(__file__, "../../test_sets/metrics_test.db")
    connection_string = f"sqlite:///{sqlite_path}"
    engine = SqlAlchemyExecutionEngine(
        connection_string=connection_string,
        create_temp_table=False,
    )
    execution_engine = engine
    my_data_connector: ConfiguredAssetSqlDataConnector = (
        ConfiguredAssetSqlDataConnector(
            name="my_sql_data_connector",
            datasource_name="my_test_datasource",
            execution_engine=execution_engine,
            assets={
                "my_asset": {
                    "table_name": "animal_names",
                },
            },
        )
    )

    context.datasources["my_test_datasource"] = Datasource(
        name="my_test_datasource",
        execution_engine=execution_engine.config,
        data_connectors={
            "my_sql_data_connector": {
                "class_name": "ConfiguredAssetSqlDataConnector",
                "assets": {
                    "my_asset": {
                        "table_name": "animal_names",
                    },
                },
            },
        },
    )

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="my_test_datasource",
                data_connector_name="my_sql_data_connector",
                data_asset_name="my_asset",
            )
        )
    )
    assert len(batch_definition_list) == 1
    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[0]
    )
    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=batch_spec
    )
    batch = Batch(data=batch_data, batch_definition=batch_definition_list[0])
    validator = Validator(
        execution_engine=engine,
        data_context=context,
        batches=[
            batch,
        ],
    )
    result = expectation.validate(validator)
    return result


def test_get_table_metric_provider_metric_dependencies(empty_sqlite_db):
    mp = ColumnMax()
    metric = MetricConfiguration(
        metric_name="column.max", metric_domain_kwargs={}, metric_value_kwargs=None
    )
    dependencies = mp.get_evaluation_dependencies(
        metric, execution_engine=SqlAlchemyExecutionEngine(engine=empty_sqlite_db)
    )
    assert (
        dependencies["metric_partial_fn"].id[0]
        == f"column.max.{MetricPartialFunctionTypes.AGGREGATE_FN.value}"
    )

    mp = ColumnMax()
    metric = MetricConfiguration(
        metric_name="column.max", metric_domain_kwargs={}, metric_value_kwargs=None
    )
    dependencies = mp.get_evaluation_dependencies(
        metric, execution_engine=PandasExecutionEngine()
    )

    table_column_types_metric: MetricConfiguration = dependencies["table.column_types"]
    table_columns_metric: MetricConfiguration = dependencies["table.columns"]
    table_row_count_metric: MetricConfiguration = dependencies["table.row_count"]
    assert dependencies == {
        "table.column_types": table_column_types_metric,
        "table.columns": table_columns_metric,
        "table.row_count": table_row_count_metric,
    }
    assert dependencies["table.columns"].id == (
        "table.columns",
        (),
        (),
    )


def test_get_aggregate_count_aware_metric_dependencies(basic_spark_df_execution_engine):
    mp = ColumnValuesNonNull()
    metric = MetricConfiguration(
        metric_name="column_values.nonnull.unexpected_count",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    dependencies = mp.get_evaluation_dependencies(
        metric, execution_engine=PandasExecutionEngine()
    )
    assert (
        dependencies["unexpected_condition"].id[0] == "column_values.nonnull.condition"
    )

    metric = MetricConfiguration(
        metric_name="column_values.nonnull.unexpected_count",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    dependencies = mp.get_evaluation_dependencies(
        metric, execution_engine=basic_spark_df_execution_engine
    )
    assert (
        dependencies["metric_partial_fn"].id[0]
        == f"column_values.nonnull.unexpected_count.{MetricPartialFunctionTypes.AGGREGATE_FN.value}"
    )

    metric = MetricConfiguration(
        metric_name=f"column_values.nonnull.unexpected_count.{MetricPartialFunctionTypes.AGGREGATE_FN.value}",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    dependencies = mp.get_evaluation_dependencies(metric)
    assert (
        dependencies["unexpected_condition"].id[0] == "column_values.nonnull.condition"
    )


def test_get_map_metric_dependencies():
    mp = ColumnMapMetricProvider()
    metric = MetricConfiguration(
        metric_name="foo.unexpected_count",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    dependencies = mp.get_evaluation_dependencies(metric)
    assert dependencies["unexpected_condition"].id[0] == "foo.condition"

    metric = MetricConfiguration(
        metric_name="foo.unexpected_rows",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    dependencies = mp.get_evaluation_dependencies(metric)
    assert dependencies["unexpected_condition"].id[0] == "foo.condition"

    metric = MetricConfiguration(
        metric_name="foo.unexpected_values",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    dependencies = mp.get_evaluation_dependencies(metric)
    assert dependencies["unexpected_condition"].id[0] == "foo.condition"

    metric = MetricConfiguration(
        metric_name="foo.unexpected_value_counts",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    dependencies = mp.get_evaluation_dependencies(metric)
    assert dependencies["unexpected_condition"].id[0] == "foo.condition"

    metric = MetricConfiguration(
        metric_name="foo.unexpected_index_list",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    dependencies = mp.get_evaluation_dependencies(metric)
    assert dependencies["unexpected_condition"].id[0] == "foo.condition"


def test_is_sqlalchemy_metric_selectable():
    assert MapMetricProvider.is_sqlalchemy_metric_selectable(
        map_metric_provider=CompoundColumnsUnique
    )

    assert not MapMetricProvider.is_sqlalchemy_metric_selectable(
        map_metric_provider=ColumnValuesNonNull
    )


def test_pandas_unexpected_rows_basic_result_format(
    in_memory_runtime_context,
    pandas_animals_dataframe_for_unexpected_rows_and_index,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "mostly": 0.9,
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "BASIC",
                "include_unexpected_rows": True,
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_pandas(
            expectation_configuration=expectation_configuration,
            dataframe=pandas_animals_dataframe_for_unexpected_rows_and_index,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_count": 3,
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
        "unexpected_rows": [
            {"animals": "giraffe", "pk_1": 3, "pk_2": "three"},
            {"animals": "lion", "pk_1": 4, "pk_2": "four"},
            {"animals": "zebra", "pk_1": 5, "pk_2": "five"},
        ],
    }


def test_pandas_unexpected_rows_summary_result_format_unexpected_rows_explicitly_false(
    in_memory_runtime_context,
    pandas_animals_dataframe_for_unexpected_rows_and_index,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "mostly": 0.9,
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "SUMMARY",  # SUMMARY will include partial_unexpected* values only
                "include_unexpected_rows": False,  # this is the default value, but making explicit for testing purposes
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_pandas(
            expectation_configuration=expectation_configuration,
            dataframe=pandas_animals_dataframe_for_unexpected_rows_and_index,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [
            {"count": 1, "value": "giraffe"},
            {"count": 1, "value": "lion"},
            {"count": 1, "value": "zebra"},
        ],
        "partial_unexpected_index_list": [3, 4, 5],
        "partial_unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_count": 3,
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_pandas_unexpected_rows_summary_result_format_unexpected_rows_including_unexpected_rows(
    in_memory_runtime_context,
    pandas_animals_dataframe_for_unexpected_rows_and_index,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "mostly": 0.9,
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "SUMMARY",  # SUMMARY will include partial_unexpected* values only
                "include_unexpected_rows": True,
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_pandas(
            expectation_configuration=expectation_configuration,
            dataframe=pandas_animals_dataframe_for_unexpected_rows_and_index,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [
            {"count": 1, "value": "giraffe"},
            {"count": 1, "value": "lion"},
            {"count": 1, "value": "zebra"},
        ],
        "partial_unexpected_index_list": [3, 4, 5],
        "partial_unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_count": 3,
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
        "unexpected_rows": [
            {"animals": "giraffe", "pk_1": 3, "pk_2": "three"},
            {"animals": "lion", "pk_1": 4, "pk_2": "four"},
            {"animals": "zebra", "pk_1": 5, "pk_2": "five"},
        ],
    }


def test_pandas_unexpected_rows_complete_result_format(
    in_memory_runtime_context,
    pandas_animals_dataframe_for_unexpected_rows_and_index,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "COMPLETE",
                "include_unexpected_rows": True,
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_pandas(
            expectation_configuration=expectation_configuration,
            dataframe=pandas_animals_dataframe_for_unexpected_rows_and_index,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [
            {"count": 1, "value": "giraffe"},
            {"count": 1, "value": "lion"},
            {"count": 1, "value": "zebra"},
        ],
        "partial_unexpected_index_list": [3, 4, 5],
        "partial_unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_count": 3,
        "unexpected_index_list": [3, 4, 5],
        "unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
        "unexpected_rows": [
            {"animals": "giraffe", "pk_1": 3, "pk_2": "three"},
            {"animals": "lion", "pk_1": 4, "pk_2": "four"},
            {"animals": "zebra", "pk_1": 5, "pk_2": "five"},
        ],
    }


def test_pandas_default_complete_result_format(
    in_memory_runtime_context,
    pandas_animals_dataframe_for_unexpected_rows_and_index: pd.DataFrame,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "COMPLETE",
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_pandas(
            expectation_configuration=expectation_configuration,
            dataframe=pandas_animals_dataframe_for_unexpected_rows_and_index,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [
            {"count": 1, "value": "giraffe"},
            {"count": 1, "value": "lion"},
            {"count": 1, "value": "zebra"},
        ],
        "partial_unexpected_index_list": [3, 4, 5],
        "partial_unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_count": 3,
        "unexpected_index_list": [3, 4, 5],
        "unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_pandas_single_unexpected_index_column_names_complete_result_format(
    in_memory_runtime_context,
    pandas_animals_dataframe_for_unexpected_rows_and_index: pd.DataFrame,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["pk_1"],  # Single column
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_pandas(
            expectation_configuration=expectation_configuration,
            dataframe=pandas_animals_dataframe_for_unexpected_rows_and_index,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [
            {"count": 1, "value": "giraffe"},
            {"count": 1, "value": "lion"},
            {"count": 1, "value": "zebra"},
        ],
        "partial_unexpected_index_list": [
            {"pk_1": 3},
            {"pk_1": 4},
            {"pk_1": 5},
        ],  # Dict since a column was provided
        "partial_unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_count": 3,
        "unexpected_index_list": [
            {"pk_1": 3},
            {"pk_1": 4},
            {"pk_1": 5},
        ],  # Dict since a column was provided
        "unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_pandas_multiple_unexpected_index_column_names_complete_result_format(
    in_memory_runtime_context,
    pandas_animals_dataframe_for_unexpected_rows_and_index: pd.DataFrame,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["pk_1", "pk_2"],  # Multiple columns
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_pandas(
            expectation_configuration=expectation_configuration,
            dataframe=pandas_animals_dataframe_for_unexpected_rows_and_index,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [
            {"count": 1, "value": "giraffe"},
            {"count": 1, "value": "lion"},
            {"count": 1, "value": "zebra"},
        ],
        "partial_unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"},
            {"pk_1": 4, "pk_2": "four"},
            {"pk_1": 5, "pk_2": "five"},
        ],  # Dicts since columns were provided
        "partial_unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_count": 3,
        "unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"},
            {"pk_1": 4, "pk_2": "four"},
            {"pk_1": 5, "pk_2": "five"},
        ],  # Dicts since columns were provided
        "unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_pandas_multiple_unexpected_index_column_names_complete_result_format_limit_1(
    in_memory_runtime_context,
    pandas_animals_dataframe_for_unexpected_rows_and_index: pd.DataFrame,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["pk_1", "pk_2"],  # Multiple columns
                "partial_unexpected_count": 1,
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_pandas(
            expectation_configuration=expectation_configuration,
            dataframe=pandas_animals_dataframe_for_unexpected_rows_and_index,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [{"count": 1, "value": "giraffe"}],
        "partial_unexpected_index_list": [{"pk_1": 3, "pk_2": "three"}],
        "partial_unexpected_list": ["giraffe"],
        "unexpected_count": 3,
        "unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"},
            {"pk_1": 4, "pk_2": "four"},
            {"pk_1": 5, "pk_2": "five"},
        ],
        "unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_pandas_multiple_unexpected_index_column_names_summary_result_format(
    in_memory_runtime_context,
    pandas_animals_dataframe_for_unexpected_rows_and_index: pd.DataFrame,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "SUMMARY",  # SUMMARY will include partial_unexpected* values only
                "unexpected_index_column_names": ["pk_1", "pk_2"],  # Multiple columns
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_pandas(
            expectation_configuration=expectation_configuration,
            dataframe=pandas_animals_dataframe_for_unexpected_rows_and_index,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [
            {"count": 1, "value": "giraffe"},
            {"count": 1, "value": "lion"},
            {"count": 1, "value": "zebra"},
        ],  # Dicts since columns were provided
        "partial_unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"},
            {"pk_1": 4, "pk_2": "four"},
            {"pk_1": 5, "pk_2": "five"},
        ],  # Dicts since columns were provided
        "partial_unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_count": 3,
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_pandas_multiple_unexpected_index_column_names_summary_result_format_limit_1(
    in_memory_runtime_context,
    pandas_animals_dataframe_for_unexpected_rows_and_index: pd.DataFrame,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "SUMMARY",  # SUMMARY will include partial_unexpected* values only
                "unexpected_index_column_names": ["pk_1", "pk_2"],  # Multiple columns
                "partial_unexpected_count": 1,
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_pandas(
            expectation_configuration=expectation_configuration,
            dataframe=pandas_animals_dataframe_for_unexpected_rows_and_index,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [{"count": 1, "value": "giraffe"}],
        "partial_unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"}
        ],  # Dicts since columns were provided
        "partial_unexpected_list": ["giraffe"],
        "unexpected_count": 3,
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_pandas_multiple_unexpected_index_column_names_basic_result_format(
    in_memory_runtime_context,
    pandas_animals_dataframe_for_unexpected_rows_and_index: pd.DataFrame,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "BASIC",  # BASIC will not include index information
                "unexpected_index_column_names": ["pk_1", "pk_2"],
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_pandas(
            expectation_configuration=expectation_configuration,
            dataframe=pandas_animals_dataframe_for_unexpected_rows_and_index,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_count": 3,
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_pandas_single_unexpected_index_column_names_complete_result_format_non_existing_column(
    in_memory_runtime_context,
    pandas_animals_dataframe_for_unexpected_rows_and_index: pd.DataFrame,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["i_dont_exist"],  # Single column
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_pandas(
            expectation_configuration=expectation_configuration,
            dataframe=pandas_animals_dataframe_for_unexpected_rows_and_index,
            context=in_memory_runtime_context,
        )
    )
    assert result.success is False
    assert result.exception_info
    assert (
        result.exception_info["exception_message"]
        == 'Error: The unexpected_index_column "i_dont_exist" does not exist in Dataframe. Please check your configuration and try again.'
    )


def test_pandas_multiple_unexpected_index_column_names_complete_result_format_non_existing_column(
    in_memory_runtime_context,
    pandas_animals_dataframe_for_unexpected_rows_and_index: pd.DataFrame,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": [
                    "pk_1",
                    "i_dont_exist",
                ],  # Only 1 column is valid
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_pandas(
            expectation_configuration=expectation_configuration,
            dataframe=pandas_animals_dataframe_for_unexpected_rows_and_index,
            context=in_memory_runtime_context,
        )
    )
    assert result.success is False
    assert result.exception_info
    assert (
        result.exception_info["exception_message"]
        == 'Error: The unexpected_index_column "i_dont_exist" does not exist in Dataframe. Please check your configuration and try again.'
    )


def test_pandas_default_to_not_include_unexpected_rows(
    in_memory_runtime_context,
    pandas_animals_dataframe_for_unexpected_rows_and_index,
    expected_evr_without_unexpected_rows,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "COMPLETE",
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_pandas(
            expectation_configuration=expectation_configuration,
            dataframe=pandas_animals_dataframe_for_unexpected_rows_and_index,
            context=in_memory_runtime_context,
        )
    )
    assert result.result == expected_evr_without_unexpected_rows.result


def test_pandas_specify_not_include_unexpected_rows(
    in_memory_runtime_context,
    pandas_animals_dataframe_for_unexpected_rows_and_index,
    expected_evr_without_unexpected_rows,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "COMPLETE",
                "include_unexpected_rows": False,
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_pandas(
            expectation_configuration=expectation_configuration,
            dataframe=pandas_animals_dataframe_for_unexpected_rows_and_index,
            context=in_memory_runtime_context,
        )
    )
    assert result.result == expected_evr_without_unexpected_rows.result


def test_include_unexpected_rows_without_explicit_result_format_raises_error(
    in_memory_runtime_context,
    pandas_animals_dataframe_for_unexpected_rows_and_index,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "include_unexpected_rows": False,
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch_definition = BatchDefinition(
        datasource_name="pandas_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="my_asset",
        batch_identifiers=IDDict({}),
        batch_spec_passthrough=None,
    )
    batch = Batch(
        data=pandas_animals_dataframe_for_unexpected_rows_and_index,
        batch_definition=batch_definition,
    )
    engine = PandasExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        data_context=in_memory_runtime_context,
        batches=[
            batch,
        ],
    )
    with pytest.raises(ValueError):
        expectation.validate(validator)


# Spark
def test_spark_single_column_complete_result_format(
    in_memory_runtime_context,
    spark_dataframe_for_unexpected_rows_with_index,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "COMPLETE",
            },
        },
    )
    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch_definition = BatchDefinition(
        datasource_name="spark_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="my_asset",
        batch_identifiers=IDDict({}),
        batch_spec_passthrough=None,
    )
    batch = Batch(
        data=spark_dataframe_for_unexpected_rows_with_index,
        batch_definition=batch_definition,
    )
    engine = SparkDFExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        data_context=in_memory_runtime_context,
        batches=[
            batch,
        ],
    )
    result = expectation.validate(validator)
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [
            {"count": 1, "value": "giraffe"},
            {"count": 1, "value": "lion"},
            {"count": 1, "value": "zebra"},
        ],
        "partial_unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_count": 3,
        "unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_spark_single_column_summary_result_format(
    in_memory_runtime_context,
    spark_dataframe_for_unexpected_rows_with_index,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "SUMMARY",
            },
        },
    )
    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch_definition = BatchDefinition(
        datasource_name="spark_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="my_asset",
        batch_identifiers=IDDict({}),
        batch_spec_passthrough=None,
    )
    batch = Batch(
        data=spark_dataframe_for_unexpected_rows_with_index,
        batch_definition=batch_definition,
    )
    engine = SparkDFExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        data_context=in_memory_runtime_context,
        batches=[
            batch,
        ],
    )
    result = expectation.validate(validator)
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [
            {"count": 1, "value": "giraffe"},
            {"count": 1, "value": "lion"},
            {"count": 1, "value": "zebra"},
        ],
        "partial_unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_count": 3,
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_sqlite_single_column_complete_result_format(
    sa,
    in_memory_runtime_context,
    sqlite_table_for_unexpected_rows_with_index,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "COMPLETE",
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_sql(
            expectation_configuration=expectation_configuration,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [
            {"count": 1, "value": "giraffe"},
            {"count": 1, "value": "lion"},
            {"count": 1, "value": "zebra"},
        ],
        "partial_unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_count": 3,
        "unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_sqlite_single_column_summary_result_format(
    sa, in_memory_runtime_context, sqlite_table_for_unexpected_rows_with_index
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "SUMMARY",
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_sql(
            expectation_configuration=expectation_configuration,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [
            {"count": 1, "value": "giraffe"},
            {"count": 1, "value": "lion"},
            {"count": 1, "value": "zebra"},
        ],
        "partial_unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_count": 3,
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_sqlite_single_unexpected_index_column_names_complete_result_format(
    sa, in_memory_runtime_context, sqlite_table_for_unexpected_rows_with_index
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["pk_1"],
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_sql(
            expectation_configuration=expectation_configuration,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [
            {"count": 1, "value": "giraffe"},
            {"count": 1, "value": "lion"},
            {"count": 1, "value": "zebra"},
        ],
        "partial_unexpected_index_list": [{"pk_1": 3}, {"pk_1": 4}, {"pk_1": 5}],
        "partial_unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_count": 3,
        "unexpected_index_list": [{"pk_1": 3}, {"pk_1": 4}, {"pk_1": 5}],
        "unexpected_index_query": "SELECT animals, pk_1 \n"
        "FROM animal_names \n"
        "WHERE animals IS NOT NULL AND (animals NOT IN "
        "('cat', 'fish', 'dog'))",
        "unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_sqlite_single_unexpected_index_column_names_summary_result_format(
    sa, in_memory_runtime_context, sqlite_table_for_unexpected_rows_with_index
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "SUMMARY",
                "unexpected_index_column_names": ["pk_1"],  # Single column
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_sql(
            expectation_configuration=expectation_configuration,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [
            {"count": 1, "value": "giraffe"},
            {"count": 1, "value": "lion"},
            {"count": 1, "value": "zebra"},
        ],
        "partial_unexpected_index_list": [{"pk_1": 3}, {"pk_1": 4}, {"pk_1": 5}],
        "partial_unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_count": 3,
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_sqlite_multiple_unexpected_index_column_names_complete_result_format(
    sa, in_memory_runtime_context, sqlite_table_for_unexpected_rows_with_index
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["pk_1", "pk_2"],  # Multiple columns
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_sql(
            expectation_configuration=expectation_configuration,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [
            {"count": 1, "value": "giraffe"},
            {"count": 1, "value": "lion"},
            {"count": 1, "value": "zebra"},
        ],
        "partial_unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"},
            {"pk_1": 4, "pk_2": "four"},
            {"pk_1": 5, "pk_2": "five"},
        ],
        "partial_unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_count": 3,
        "unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"},
            {"pk_1": 4, "pk_2": "four"},
            {"pk_1": 5, "pk_2": "five"},
        ],
        "unexpected_index_query": "SELECT animals, pk_1, pk_2 \n"
        "FROM animal_names \n"
        "WHERE animals IS NOT NULL AND (animals NOT IN "
        "('cat', 'fish', 'dog'))",
        "unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_sql_multiple_unexpected_index_column_names_complete_result_format_limit_1(
    sa, in_memory_runtime_context, sqlite_table_for_unexpected_rows_with_index
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["pk_1", "pk_2"],  # Multiple columns
                "partial_unexpected_count": 1,
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_sql(
            expectation_configuration=expectation_configuration,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [{"count": 1, "value": "giraffe"}],
        "partial_unexpected_index_list": [{"pk_1": 3, "pk_2": "three"}],
        "partial_unexpected_list": ["giraffe"],
        "unexpected_count": 3,
        "unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"},
        ],
        "unexpected_index_query": "SELECT animals, pk_1, pk_2 \n"
        "FROM animal_names \n"
        "WHERE animals IS NOT NULL AND (animals NOT IN "
        "('cat', 'fish', 'dog'))",
        "unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_sql_multiple_unexpected_index_column_names_summary_result_format(
    sa, in_memory_runtime_context, sqlite_table_for_unexpected_rows_with_index
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "SUMMARY",  # SUMMARY will include partial_unexpected* values only
                "unexpected_index_column_names": ["pk_1", "pk_2"],  # Multiple columns
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_sql(
            expectation_configuration=expectation_configuration,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [
            {"count": 1, "value": "giraffe"},
            {"count": 1, "value": "lion"},
            {"count": 1, "value": "zebra"},
        ],
        "partial_unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"},
            {"pk_1": 4, "pk_2": "four"},
            {"pk_1": 5, "pk_2": "five"},
        ],
        "partial_unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_count": 3,
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_sql_multiple_unexpected_index_column_names_summary_result_format_limit_1(
    sa, in_memory_runtime_context, sqlite_table_for_unexpected_rows_with_index
):

    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "SUMMARY",  # SUMMARY will include partial_unexpected* values only
                "unexpected_index_column_names": ["pk_1", "pk_2"],  # Multiple columns
                "partial_unexpected_count": 1,
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_sql(
            expectation_configuration=expectation_configuration,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [{"count": 1, "value": "giraffe"}],
        "partial_unexpected_index_list": [{"pk_1": 3, "pk_2": "three"}],
        "partial_unexpected_list": ["giraffe"],
        "unexpected_count": 3,
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_sql_multiple_unexpected_index_column_names_basic_result_format(
    sa, in_memory_runtime_context, sqlite_table_for_unexpected_rows_with_index
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "BASIC",  # SUMMARY will include partial_unexpected_list only, which means unexpected_index_column_names will have no effect
                "unexpected_index_column_names": ["pk_1", "pk_2"],
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_sql(
            expectation_configuration=expectation_configuration,
            context=in_memory_runtime_context,
        )
    )
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_count": 3,
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_sql_single_unexpected_index_column_names_complete_result_format_non_existing_column(
    sa, in_memory_runtime_context, sqlite_table_for_unexpected_rows_with_index
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["i_dont_exist"],  # Single column
            },
        },
    )
    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_sql(
            expectation_configuration=expectation_configuration,
            context=in_memory_runtime_context,
        )
    )

    assert result.success is False
    assert result.exception_info
    assert (
        result.exception_info["exception_message"]
        == 'Error: The unexpected_index_column: "i_dont_exist" in does not exist in SQL Table. Please check your configuration and try again.'
    )


def test_sql_multiple_unexpected_index_column_names_complete_result_format_non_existing_column(
    sa, in_memory_runtime_context, sqlite_table_for_unexpected_rows_with_index
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": [
                    "pk_1",
                    "i_dont_exist",
                ],  # Only 1 column is valid
            },
        },
    )

    result: ExpectationValidationResult = (
        _expecation_configuration_to_validation_result_sql(
            expectation_configuration=expectation_configuration,
            context=in_memory_runtime_context,
        )
    )
    assert result.success is False
    assert result.exception_info
    assert (
        result.exception_info["exception_message"]
        == 'Error: The unexpected_index_column: "i_dont_exist" in does not exist in SQL Table. Please check your configuration and try again.'
    )
