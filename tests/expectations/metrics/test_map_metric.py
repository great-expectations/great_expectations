import pandas as pd
import pytest

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.core.batch import Batch, BatchRequest
from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource.data_connector import ConfiguredAssetSqlDataConnector
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SqlAlchemyExecutionEngine,
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
def dataframe_for_unexpected_rows():
    return pd.DataFrame(
        {
            "a": [1, 5, 22, 3, 5, 10],
            "b": ["cat", "fish", "dog", "giraffe", "lion", "zebra"],
        }
    )


@pytest.fixture
def pandas_dataframe_for_unexpected_rows_with_index():
    return pd.DataFrame(
        {
            "pk_1": [0, 1, 2, 3, 4, 5],
            "pk_2": ["zero", "one", "two", "three", "four", "five"],
            "numbers_with_duplicates": [1, 5, 22, 3, 5, 10],
            "animal_names_no_duplicates": [
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
def sqlite_table_for_unexpected_rows_with_index(test_backends):
    # Create a small in-memory engine with two views, one of which is temporary
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
                    "numbers_with_duplicates": [1, 5, 22, 3, 5, 10],
                    "animal_names_no_duplicates": [
                        "cat",
                        "fish",
                        "dog",
                        "giraffe",
                        "lion",
                        "zebra",
                    ],
                }
            )
            # sqlite_engine.execute(
            #     """CREATE TEMP TABLE test_temp(
            #        pk_1 INT,
            #        pk_2 TEXT,
            #        numbers_with_duplicates INT,
            #        animal_names_no_duplicates TEXT
            #     );"""
            # )
            # # may need this later for creating temp tables
            # sqlite_engine.execute("""CREATE INDEX index_name ON test_temp (pk_1);""")
            df.to_sql(
                name="test_temp", con=sqlite_engine, index=False, if_exists="replace"
            )
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
                "column": "a",
                "value_set": [1, 5, 22],
            },
            "meta": {},
        },
        result={
            "element_count": 6,
            "unexpected_count": 2,
            "unexpected_index_list": [3, 5],
            "unexpected_percent": 33.33333333333333,
            "partial_unexpected_list": [3, 10],
            "unexpected_list": [3, 10],
            "partial_unexpected_index_list": [3, 5],
            "partial_unexpected_counts": [
                {"value": 3, "count": 1},
                {"value": 10, "count": 1},
            ],
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent_total": 33.33333333333333,
            "unexpected_percent_nonmissing": 33.33333333333333,
        },
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        meta={},
    )


def test_get_table_metric_provider_metric_dependencies(empty_sqlite_db):
    mp = ColumnMax()
    metric = MetricConfiguration(
        metric_name="column.max", metric_domain_kwargs={}, metric_value_kwargs=None
    )
    dependencies = mp.get_evaluation_dependencies(
        metric, execution_engine=SqlAlchemyExecutionEngine(engine=empty_sqlite_db)
    )
    assert dependencies["metric_partial_fn"].id[0] == "column.max.aggregate_fn"

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
        == "column_values.nonnull.unexpected_count.aggregate_fn"
    )

    metric = MetricConfiguration(
        metric_name="column_values.nonnull.unexpected_count.aggregate_fn",
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


def test_pandas_unexpected_rows_basic_result_format(dataframe_for_unexpected_rows):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "b",
            "mostly": 0.9,
            "value_set": ["cat", "fish", "dog", "giraffe"],
            "result_format": {
                "result_format": "BASIC",
                "include_unexpected_rows": True,
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch = Batch(data=dataframe_for_unexpected_rows)
    engine = PandasExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        batches=[
            batch,
        ],
    )
    result = expectation.validate(validator)

    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "unexpected_count": 2,
        "unexpected_percent": 33.33333333333333,
        "partial_unexpected_list": ["lion", "zebra"],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
        "unexpected_rows": [{"a": 5, "b": "lion"}, {"a": 10, "b": "zebra"}],
    }


def test_pandas_unexpected_rows_summary_result_format_unexpected_rows_explicitly_false(
    dataframe_for_unexpected_rows,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "b",
            "mostly": 0.9,
            "value_set": ["cat", "fish", "dog", "giraffe"],
            "result_format": {
                "result_format": "SUMMARY",
                "include_unexpected_rows": False,  # this is the default value, but making explicit for testing purposes
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch = Batch(data=dataframe_for_unexpected_rows)
    engine = PandasExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        batches=[
            batch,
        ],
    )
    result = expectation.validate(validator)

    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "unexpected_count": 2,
        "unexpected_percent": 33.33333333333333,
        "partial_unexpected_counts": [
            {"count": 1, "value": "lion"},
            {"count": 1, "value": "zebra"},
        ],
        "partial_unexpected_index_list": [4, 5],
        "partial_unexpected_list": ["lion", "zebra"],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
    }


def test_pandas_unexpected_rows_summary_result_format_unexpected_rows_including_unexpected_rows(
    dataframe_for_unexpected_rows,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "b",
            "mostly": 0.9,
            "value_set": ["cat", "fish", "dog", "giraffe"],
            "result_format": {
                "result_format": "SUMMARY",
                "include_unexpected_rows": True,
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch = Batch(data=dataframe_for_unexpected_rows)
    engine = PandasExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        batches=[
            batch,
        ],
    )
    result = expectation.validate(validator)

    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "unexpected_count": 2,
        "unexpected_percent": 33.33333333333333,
        "partial_unexpected_counts": [
            {"count": 1, "value": "lion"},
            {"count": 1, "value": "zebra"},
        ],
        "partial_unexpected_index_list": [4, 5],
        "partial_unexpected_list": ["lion", "zebra"],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
        "unexpected_rows": [{"a": 5, "b": "lion"}, {"a": 10, "b": "zebra"}],
    }


def test_pandas_unexpected_rows_complete_result_format(dataframe_for_unexpected_rows):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "a",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "COMPLETE",
                "include_unexpected_rows": True,
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch = Batch(data=dataframe_for_unexpected_rows)
    engine = PandasExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        batches=[
            batch,
        ],
    )
    result = expectation.validate(validator)
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "unexpected_count": 2,
        "unexpected_index_list": [3, 5],
        "unexpected_percent": 33.33333333333333,
        "partial_unexpected_list": [3, 10],
        "unexpected_list": [3, 10],
        "unexpected_rows": [{"a": 3, "b": "giraffe"}, {"a": 10, "b": "zebra"}],
        "partial_unexpected_index_list": [3, 5],
        "partial_unexpected_counts": [
            {"value": 3, "count": 1},
            {"value": 10, "count": 1},
        ],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
    }


def test_pandas_default_complete_result_format(
    pandas_dataframe_for_unexpected_rows_with_index: pd.DataFrame,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "numbers_with_duplicates",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "COMPLETE",
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch: Batch = Batch(data=pandas_dataframe_for_unexpected_rows_with_index)
    engine = PandasExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        batches=[
            batch,
        ],
    )
    result = expectation.validate(validator)
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "unexpected_count": 2,
        "unexpected_index_list": [
            3,
            5,
        ],  # Default pandas index applied, compatible with existing functionality (list of ints)
        "partial_unexpected_index_list": [3, 5],
        "unexpected_percent": 33.33333333333333,
        "partial_unexpected_list": [3, 10],
        "unexpected_list": [3, 10],
        "partial_unexpected_counts": [
            {"value": 3, "count": 1},
            {"value": 10, "count": 1},
        ],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
    }


def test_pandas_single_unexpected_index_column_names_complete_result_format(
    pandas_dataframe_for_unexpected_rows_with_index: pd.DataFrame,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "numbers_with_duplicates",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["pk_1"],  # Single column
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch: Batch = Batch(data=pandas_dataframe_for_unexpected_rows_with_index)
    engine = PandasExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        batches=[
            batch,
        ],
    )
    result = expectation.validate(validator)
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "unexpected_count": 2,
        "unexpected_index_list": [
            {
                "pk_1": 3,
            },
            {
                "pk_1": 5,
            },
        ],  # Dicts since a column was provided
        "partial_unexpected_index_list": [
            {
                "pk_1": 3,
            },
            {
                "pk_1": 5,
            },
        ],  # Dicts since a column was provided
        "unexpected_percent": 33.33333333333333,
        "partial_unexpected_list": [3, 10],
        "unexpected_list": [3, 10],
        "partial_unexpected_counts": [
            {"value": 3, "count": 1},
            {"value": 10, "count": 1},
        ],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
    }


def test_pandas_multiple_unexpected_index_column_names_complete_result_format(
    pandas_dataframe_for_unexpected_rows_with_index: pd.DataFrame,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "numbers_with_duplicates",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["pk_1", "pk_2"],  # Multiple columns
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch: Batch = Batch(data=pandas_dataframe_for_unexpected_rows_with_index)
    engine = PandasExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        batches=[
            batch,
        ],
    )
    result = expectation.validate(validator)
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "unexpected_count": 2,
        "unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"},
            {"pk_1": 5, "pk_2": "five"},
        ],  # Dicts since columns were provided
        "partial_unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"},
            {"pk_1": 5, "pk_2": "five"},
        ],  # Dicts since columns were provided
        "unexpected_percent": 33.33333333333333,
        "partial_unexpected_list": [3, 10],
        "unexpected_list": [3, 10],
        "partial_unexpected_counts": [
            {"value": 3, "count": 1},
            {"value": 10, "count": 1},
        ],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
    }


def test_pandas_multiple_unexpected_index_column_names_complete_result_format_limit_1(
    pandas_dataframe_for_unexpected_rows_with_index: pd.DataFrame,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "numbers_with_duplicates",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["pk_1", "pk_2"],  # Multiple columns
                "partial_unexpected_count": 1,
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch: Batch = Batch(data=pandas_dataframe_for_unexpected_rows_with_index)
    engine = PandasExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        batches=[
            batch,
        ],
    )
    result = expectation.validate(validator)
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "unexpected_count": 2,
        "unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"},
            {"pk_1": 5, "pk_2": "five"},
        ],  # Dicts since columns were provided
        "partial_unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"},
        ],  # Dicts since columns were provided
        "unexpected_percent": 33.33333333333333,
        "partial_unexpected_list": [3],
        "unexpected_list": [3, 10],
        "partial_unexpected_counts": [
            {"value": 3, "count": 1},
        ],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
    }


def test_pandas_multiple_unexpected_index_column_names_summary_result_format(
    pandas_dataframe_for_unexpected_rows_with_index: pd.DataFrame,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "numbers_with_duplicates",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "SUMMARY",  # SUMMARY will include partial_unexpected* values only
                "unexpected_index_column_names": ["pk_1", "pk_2"],  # Multiple columns
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch: Batch = Batch(data=pandas_dataframe_for_unexpected_rows_with_index)
    engine = PandasExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        batches=[
            batch,
        ],
    )
    result = expectation.validate(validator)
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "unexpected_count": 2,
        "partial_unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"},
            {"pk_1": 5, "pk_2": "five"},
        ],  # Dicts since columns were provided
        "unexpected_percent": 33.33333333333333,
        "partial_unexpected_list": [3, 10],
        "partial_unexpected_counts": [
            {"value": 3, "count": 1},
            {"value": 10, "count": 1},
        ],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
    }


def test_pandas_multiple_unexpected_index_column_names_summary_result_format_limit_1(
    pandas_dataframe_for_unexpected_rows_with_index: pd.DataFrame,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "numbers_with_duplicates",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "SUMMARY",  # SUMMARY will include partial_unexpected* values only
                "unexpected_index_column_names": ["pk_1", "pk_2"],  # Multiple columns
                "partial_unexpected_count": 1,
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch: Batch = Batch(data=pandas_dataframe_for_unexpected_rows_with_index)
    engine = PandasExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        batches=[
            batch,
        ],
    )
    result = expectation.validate(validator)
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "unexpected_count": 2,
        "partial_unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"},
        ],  # Dicts since columns were provided
        "unexpected_percent": 33.33333333333333,
        "partial_unexpected_list": [3],
        "partial_unexpected_counts": [
            {"value": 3, "count": 1},
        ],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
    }


def test_pandas_multiple_unexpected_index_column_names_basic_result_format(
    pandas_dataframe_for_unexpected_rows_with_index: pd.DataFrame,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "numbers_with_duplicates",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "BASIC",  # SUMMARY will include partial_unexpected_list only, which means unexpected_index_column_names will have no effect
                "unexpected_index_column_names": ["pk_1", "pk_2"],
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch: Batch = Batch(data=pandas_dataframe_for_unexpected_rows_with_index)
    engine = PandasExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        batches=[
            batch,
        ],
    )
    result = expectation.validate(validator)
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "unexpected_count": 2,
        "unexpected_percent": 33.33333333333333,
        "partial_unexpected_list": [3, 10],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
    }


def test_pandas_single_unexpected_index_column_names_complete_result_format_non_existing_column(
    pandas_dataframe_for_unexpected_rows_with_index: pd.DataFrame,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "numbers_with_duplicates",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["i_dont_exist"],  # Single column
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch: Batch = Batch(data=pandas_dataframe_for_unexpected_rows_with_index)
    engine = PandasExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        batches=[
            batch,
        ],
    )
    result = expectation.validate(validator)
    assert result.success is False
    assert result.exception_info
    assert (
        result.exception_info["exception_message"]
        == 'Error: The unexpected_index_column: "i_dont_exist" does not exist in Dataframe. Please check your configuration and try again.'
    )


def test_pandas_multiple_unexpected_index_column_names_complete_result_format_non_existing_column(
    pandas_dataframe_for_unexpected_rows_with_index: pd.DataFrame,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "numbers_with_duplicates",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": [
                    "pk_1",
                    "i_dont_exist",
                ],  # Only 1 column is valid
            },
        },
    )
    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch: Batch = Batch(data=pandas_dataframe_for_unexpected_rows_with_index)
    engine = PandasExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        batches=[
            batch,
        ],
    )
    result = expectation.validate(validator)
    assert result.success is False
    assert result.exception_info
    assert (
        result.exception_info["exception_message"]
        == 'Error: The unexpected_index_column: "i_dont_exist" does not exist in Dataframe. Please check your configuration and try again.'
    )


def test_pandas_default_to_not_include_unexpected_rows(
    dataframe_for_unexpected_rows, expected_evr_without_unexpected_rows
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "a",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "COMPLETE",
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch = Batch(data=dataframe_for_unexpected_rows)
    engine = PandasExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        batches=[
            batch,
        ],
    )
    result = expectation.validate(validator)
    assert result.result == expected_evr_without_unexpected_rows.result


def test_pandas_specify_not_include_unexpected_rows(
    dataframe_for_unexpected_rows, expected_evr_without_unexpected_rows
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "a",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "COMPLETE",
                "include_unexpected_rows": False,
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch = Batch(data=dataframe_for_unexpected_rows)
    engine = PandasExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        batches=[
            batch,
        ],
    )
    result = expectation.validate(validator)
    assert result.result == expected_evr_without_unexpected_rows.result


def test_include_unexpected_rows_without_explicit_result_format_raises_error(
    dataframe_for_unexpected_rows,
):
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "a",
            "value_set": [1, 5, 22],
            "result_format": {
                "include_unexpected_rows": False,
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch = Batch(data=dataframe_for_unexpected_rows)
    engine = PandasExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        batches=[
            batch,
        ],
    )
    with pytest.raises(ValueError):
        expectation.validate(validator)


def test_sqlite_single_column_complete_result_format(
    sa, sqlite_table_for_unexpected_rows_with_index
):
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "numbers_with_duplicates",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "COMPLETE",
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectationConfiguration)
    sqlite_path = file_relative_path(__file__, "../../test_sets/metrics_test.db")
    connection_string = f"sqlite:///{sqlite_path}"
    engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
    execution_engine = engine
    my_data_connector: ConfiguredAssetSqlDataConnector = (
        ConfiguredAssetSqlDataConnector(
            name="my_sql_data_connector",
            datasource_name="my_test_datasource",
            execution_engine=execution_engine,
            assets={
                "my_asset": {
                    "table_name": "test_temp",
                },
            },
        )
    )
    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            # data_asset_name="main.my_asset"
            data_asset_name="my_asset",
        )
    )
    assert len(batch_definition_list) == 1
    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[0]
    )
    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=batch_spec
    )
    batch = Batch(data=batch_data)
    validator = Validator(execution_engine, batches=[batch])
    result = expectation.validate(validator)
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "unexpected_count": 2,
        "unexpected_percent": 33.33333333333333,
        "partial_unexpected_list": [3, 10],
        "unexpected_list": [3, 10],
        "partial_unexpected_counts": [
            {"value": 3, "count": 1},
            {"value": 10, "count": 1},
        ],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
    }


def test_sqlite_single_unexpected_index_column_names_complete_result_format(
    sa, sqlite_table_for_unexpected_rows_with_index
):
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "numbers_with_duplicates",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["pk_1"],  # Single column
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectationConfiguration)
    sqlite_path = file_relative_path(__file__, "../../test_sets/metrics_test.db")
    connection_string = f"sqlite:///{sqlite_path}"
    engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
    execution_engine = engine
    my_data_connector: ConfiguredAssetSqlDataConnector = (
        ConfiguredAssetSqlDataConnector(
            name="my_sql_data_connector",
            datasource_name="my_test_datasource",
            execution_engine=execution_engine,
            assets={
                "my_asset": {
                    "table_name": "test_temp",
                },
            },
        )
    )
    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            # data_asset_name="main.my_asset"
            data_asset_name="my_asset",
        )
    )
    assert len(batch_definition_list) == 1
    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[0]
    )
    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=batch_spec
    )
    batch = Batch(data=batch_data)
    validator = Validator(execution_engine, batches=[batch])
    result = expectation.validate(validator)
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [
            {"count": 1, "value": 3},
            {"count": 1, "value": 10},
        ],
        "partial_unexpected_index_list": [{"pk_1": 3}, {"pk_1": 5}],
        "partial_unexpected_list": [3, 10],
        "unexpected_count": 2,
        "unexpected_index_list": [{"pk_1": 3}, {"pk_1": 5}],
        "unexpected_index_query": "SELECT numbers_with_duplicates, pk_1 \n"
        "FROM test_temp \n"
        "WHERE numbers_with_duplicates IS NOT NULL AND "
        "(numbers_with_duplicates NOT IN (1, 5, 22))",
        "unexpected_list": [3, 10],
        "unexpected_percent": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
        "unexpected_percent_total": 33.33333333333333,
    }


def test_sqlite_single_unexpected_index_column_names_summary_result_format(
    sa, sqlite_table_for_unexpected_rows_with_index
):
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "numbers_with_duplicates",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "SUMMARY",
                "unexpected_index_column_names": ["pk_1"],  # Single column
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectationConfiguration)
    sqlite_path = file_relative_path(__file__, "../../test_sets/metrics_test.db")
    connection_string = f"sqlite:///{sqlite_path}"
    engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
    execution_engine = engine
    my_data_connector: ConfiguredAssetSqlDataConnector = (
        ConfiguredAssetSqlDataConnector(
            name="my_sql_data_connector",
            datasource_name="my_test_datasource",
            execution_engine=execution_engine,
            assets={
                "my_asset": {
                    "table_name": "test_temp",
                },
            },
        )
    )
    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            # data_asset_name="main.my_asset"
            data_asset_name="my_asset",
        )
    )
    assert len(batch_definition_list) == 1
    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[0]
    )
    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=batch_spec
    )
    batch = Batch(data=batch_data)
    validator = Validator(execution_engine, batches=[batch])
    result = expectation.validate(validator)
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "unexpected_count": 2,
        "partial_unexpected_index_list": [
            {
                "pk_1": 3,
            },
            {
                "pk_1": 5,
            },
        ],  # Dicts since a column was provided
        "unexpected_percent": 33.33333333333333,
        "partial_unexpected_list": [3, 10],
        "partial_unexpected_counts": [
            {"value": 3, "count": 1},
            {"value": 10, "count": 1},
        ],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
    }


def test_sqlite_multiple_unexpected_index_column_names_complete_result_format(
    sa, sqlite_table_for_unexpected_rows_with_index
):
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "numbers_with_duplicates",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["pk_1", "pk_2"],  # Multiple columns
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectationConfiguration)
    sqlite_path = file_relative_path(__file__, "../../test_sets/metrics_test.db")
    connection_string = f"sqlite:///{sqlite_path}"
    engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
    execution_engine = engine
    my_data_connector: ConfiguredAssetSqlDataConnector = (
        ConfiguredAssetSqlDataConnector(
            name="my_sql_data_connector",
            datasource_name="my_test_datasource",
            execution_engine=execution_engine,
            assets={
                "my_asset": {
                    "table_name": "test_temp",
                },
            },
        )
    )
    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            # data_asset_name="main.my_asset"
            data_asset_name="my_asset",
        )
    )
    assert len(batch_definition_list) == 1
    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[0]
    )
    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=batch_spec
    )
    batch = Batch(data=batch_data)
    validator = Validator(execution_engine, batches=[batch])
    result = expectation.validate(validator)
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [
            {"count": 1, "value": 3},
            {"count": 1, "value": 10},
        ],
        "partial_unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"},
            {"pk_1": 5, "pk_2": "five"},
        ],
        "partial_unexpected_list": [3, 10],
        "unexpected_count": 2,
        "unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"},
            {"pk_1": 5, "pk_2": "five"},
        ],
        "unexpected_index_query": "SELECT numbers_with_duplicates, pk_1, pk_2 \n"
        "FROM test_temp \n"
        "WHERE numbers_with_duplicates IS NOT NULL AND "
        "(numbers_with_duplicates NOT IN (1, 5, 22))",
        "unexpected_list": [3, 10],
        "unexpected_percent": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
        "unexpected_percent_total": 33.33333333333333,
    }


def test_sql_multiple_unexpected_index_column_names_complete_result_format_limit_1(
    sa, sqlite_table_for_unexpected_rows_with_index
):
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "numbers_with_duplicates",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["pk_1", "pk_2"],  # Multiple columns
                "partial_unexpected_count": 1,
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectationConfiguration)
    sqlite_path = file_relative_path(__file__, "../../test_sets/metrics_test.db")
    connection_string = f"sqlite:///{sqlite_path}"
    engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
    execution_engine = engine
    my_data_connector: ConfiguredAssetSqlDataConnector = (
        ConfiguredAssetSqlDataConnector(
            name="my_sql_data_connector",
            datasource_name="my_test_datasource",
            execution_engine=execution_engine,
            assets={
                "my_asset": {
                    "table_name": "test_temp",
                },
            },
        )
    )
    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            # data_asset_name="main.my_asset"
            data_asset_name="my_asset",
        )
    )
    assert len(batch_definition_list) == 1
    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[0]
    )
    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=batch_spec
    )
    batch = Batch(data=batch_data)
    validator = Validator(execution_engine, batches=[batch])
    result = expectation.validate(validator)
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_counts": [{"count": 1, "value": 3}],
        "partial_unexpected_index_list": [{"pk_1": 3, "pk_2": "three"}],
        "partial_unexpected_list": [3],
        "unexpected_count": 2,
        "unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"},
            {"pk_1": 5, "pk_2": "five"},
        ],
        "unexpected_index_query": "SELECT numbers_with_duplicates, pk_1, pk_2 \n"
        "FROM test_temp \n"
        "WHERE numbers_with_duplicates IS NOT NULL AND "
        "(numbers_with_duplicates NOT IN (1, 5, 22))",
        "unexpected_list": [3, 10],
        "unexpected_percent": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
        "unexpected_percent_total": 33.33333333333333,
    }


def test_sql_multiple_unexpected_index_column_names_summary_result_format(
    sa, sqlite_table_for_unexpected_rows_with_index
):
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "numbers_with_duplicates",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "SUMMARY",  # SUMMARY will include partial_unexpected* values only
                "unexpected_index_column_names": ["pk_1", "pk_2"],  # Multiple columns
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectationConfiguration)
    sqlite_path = file_relative_path(__file__, "../../test_sets/metrics_test.db")
    connection_string = f"sqlite:///{sqlite_path}"
    engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
    execution_engine = engine
    my_data_connector: ConfiguredAssetSqlDataConnector = (
        ConfiguredAssetSqlDataConnector(
            name="my_sql_data_connector",
            datasource_name="my_test_datasource",
            execution_engine=execution_engine,
            assets={
                "my_asset": {
                    "table_name": "test_temp",
                },
            },
        )
    )
    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            # data_asset_name="main.my_asset"
            data_asset_name="my_asset",
        )
    )
    assert len(batch_definition_list) == 1
    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[0]
    )
    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=batch_spec
    )
    batch = Batch(data=batch_data)
    validator = Validator(execution_engine, batches=[batch])
    result = expectation.validate(validator)
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "unexpected_count": 2,
        "partial_unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"},
            {"pk_1": 5, "pk_2": "five"},
        ],  # Dicts since columns were provided
        "unexpected_percent": 33.33333333333333,
        "partial_unexpected_list": [3, 10],
        "partial_unexpected_counts": [
            {"value": 3, "count": 1},
            {"value": 10, "count": 1},
        ],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
    }


def test_sql_multiple_unexpected_index_column_names_summary_result_format_limit_1(
    sa, sqlite_table_for_unexpected_rows_with_index
):
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "numbers_with_duplicates",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "SUMMARY",  # SUMMARY will include partial_unexpected* values only
                "unexpected_index_column_names": ["pk_1", "pk_2"],  # Multiple columns
                "partial_unexpected_count": 1,
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectationConfiguration)
    sqlite_path = file_relative_path(__file__, "../../test_sets/metrics_test.db")
    connection_string = f"sqlite:///{sqlite_path}"
    engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
    execution_engine = engine
    my_data_connector: ConfiguredAssetSqlDataConnector = (
        ConfiguredAssetSqlDataConnector(
            name="my_sql_data_connector",
            datasource_name="my_test_datasource",
            execution_engine=execution_engine,
            assets={
                "my_asset": {
                    "table_name": "test_temp",
                },
            },
        )
    )
    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            # data_asset_name="main.my_asset"
            data_asset_name="my_asset",
        )
    )
    assert len(batch_definition_list) == 1
    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[0]
    )
    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=batch_spec
    )
    batch = Batch(data=batch_data)
    validator = Validator(execution_engine, batches=[batch])
    result = expectation.validate(validator)
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "unexpected_count": 2,
        "partial_unexpected_index_list": [
            {"pk_1": 3, "pk_2": "three"},
        ],  # Dicts since columns were provided
        "unexpected_percent": 33.33333333333333,
        "partial_unexpected_list": [3],
        "partial_unexpected_counts": [
            {"value": 3, "count": 1},
        ],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
    }


def test_sql_multiple_unexpected_index_column_names_basic_result_format(
    sa, sqlite_table_for_unexpected_rows_with_index
):
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "numbers_with_duplicates",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "BASIC",  # SUMMARY will include partial_unexpected_list only, which means unexpected_index_column_names will have no effect
                "unexpected_index_column_names": ["pk_1", "pk_2"],
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectationConfiguration)
    sqlite_path = file_relative_path(__file__, "../../test_sets/metrics_test.db")
    connection_string = f"sqlite:///{sqlite_path}"
    engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
    execution_engine = engine
    my_data_connector: ConfiguredAssetSqlDataConnector = (
        ConfiguredAssetSqlDataConnector(
            name="my_sql_data_connector",
            datasource_name="my_test_datasource",
            execution_engine=execution_engine,
            assets={
                "my_asset": {
                    "table_name": "test_temp",
                },
            },
        )
    )
    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            # data_asset_name="main.my_asset"
            data_asset_name="my_asset",
        )
    )
    assert len(batch_definition_list) == 1
    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[0]
    )
    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=batch_spec
    )
    batch = Batch(data=batch_data)
    validator = Validator(execution_engine, batches=[batch])
    result = expectation.validate(validator)
    assert convert_to_json_serializable(result.result) == {
        "element_count": 6,
        "unexpected_count": 2,
        "unexpected_percent": 33.33333333333333,
        "partial_unexpected_list": [3, 10],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
    }


def test_sql_single_unexpected_index_column_names_complete_result_format_non_existing_column(
    sa, sqlite_table_for_unexpected_rows_with_index
):
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "numbers_with_duplicates",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["i_dont_exist"],  # Single column
            },
        },
    )

    expectation = ExpectColumnValuesToBeInSet(expectationConfiguration)
    sqlite_path = file_relative_path(__file__, "../../test_sets/metrics_test.db")
    connection_string = f"sqlite:///{sqlite_path}"
    engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
    execution_engine = engine
    my_data_connector: ConfiguredAssetSqlDataConnector = (
        ConfiguredAssetSqlDataConnector(
            name="my_sql_data_connector",
            datasource_name="my_test_datasource",
            execution_engine=execution_engine,
            assets={
                "my_asset": {
                    "table_name": "test_temp",
                },
            },
        )
    )
    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            # data_asset_name="main.my_asset"
            data_asset_name="my_asset",
        )
    )
    assert len(batch_definition_list) == 1
    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[0]
    )
    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=batch_spec
    )
    batch = Batch(data=batch_data)
    validator = Validator(execution_engine, batches=[batch])
    result = expectation.validate(validator)
    assert result.success is False
    assert result.exception_info
    assert (
        result.exception_info["exception_message"]
        == 'Error: The unexpected_index_column: "i_dont_exist" in does not exist in SQL Table. Please check your configuration and try again.'
    )


def test_sql_multiple_unexpected_index_column_names_complete_result_format_non_existing_column(
    sa, sqlite_table_for_unexpected_rows_with_index
):
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "numbers_with_duplicates",
            "value_set": [1, 5, 22],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": [
                    "pk_1",
                    "i_dont_exist",
                ],  # Only 1 column is valid
            },
        },
    )
    expectation = ExpectColumnValuesToBeInSet(expectationConfiguration)
    sqlite_path = file_relative_path(__file__, "../../test_sets/metrics_test.db")
    connection_string = f"sqlite:///{sqlite_path}"
    engine = SqlAlchemyExecutionEngine(connection_string=connection_string)
    execution_engine = engine
    my_data_connector: ConfiguredAssetSqlDataConnector = (
        ConfiguredAssetSqlDataConnector(
            name="my_sql_data_connector",
            datasource_name="my_test_datasource",
            execution_engine=execution_engine,
            assets={
                "my_asset": {
                    "table_name": "test_temp",
                },
            },
        )
    )
    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            # data_asset_name="main.my_asset"
            data_asset_name="my_asset",
        )
    )
    assert len(batch_definition_list) == 1
    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[0]
    )
    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=batch_spec
    )
    batch = Batch(data=batch_data)
    validator = Validator(execution_engine, batches=[batch])
    result = expectation.validate(validator)
    assert result.success is False
    assert result.exception_info
    assert (
        result.exception_info["exception_message"]
        == 'Error: The unexpected_index_column: "i_dont_exist" in does not exist in SQL Table. Please check your configuration and try again.'
    )
