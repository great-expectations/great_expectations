import pandas as pd
import pytest

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.core.batch import Batch
from great_expectations.core.util import convert_to_json_serializable
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
def pandas_animals_dataframe():
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


def test_pandas_unexpected_rows_basic_result_format(pandas_animals_dataframe):
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

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch = Batch(data=pandas_animals_dataframe)
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
    pandas_animals_dataframe,
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

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch = Batch(data=pandas_animals_dataframe)
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
    pandas_animals_dataframe,
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

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch = Batch(data=pandas_animals_dataframe)
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


def test_pandas_unexpected_rows_complete_result_format(pandas_animals_dataframe):
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

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch = Batch(data=pandas_animals_dataframe)
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
    pandas_animals_dataframe: pd.DataFrame,
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
    batch: Batch = Batch(data=pandas_animals_dataframe)
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
    pandas_animals_dataframe: pd.DataFrame,
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

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch: Batch = Batch(data=pandas_animals_dataframe)
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
    pandas_animals_dataframe: pd.DataFrame,
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

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch: Batch = Batch(data=pandas_animals_dataframe)
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
    pandas_animals_dataframe: pd.DataFrame,
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

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch: Batch = Batch(data=pandas_animals_dataframe)
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
    pandas_animals_dataframe: pd.DataFrame,
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

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch: Batch = Batch(data=pandas_animals_dataframe)
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
    pandas_animals_dataframe: pd.DataFrame,
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

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch: Batch = Batch(data=pandas_animals_dataframe)
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
    pandas_animals_dataframe: pd.DataFrame,
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

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch: Batch = Batch(data=pandas_animals_dataframe)
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
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_list": ["giraffe", "lion", "zebra"],
        "unexpected_count": 3,
        "unexpected_percent": 50.0,
        "unexpected_percent_nonmissing": 50.0,
        "unexpected_percent_total": 50.0,
    }


def test_pandas_single_unexpected_index_column_names_complete_result_format_non_existing_column(
    pandas_animals_dataframe: pd.DataFrame,
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

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch: Batch = Batch(data=pandas_animals_dataframe)
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
    pandas_animals_dataframe: pd.DataFrame,
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
    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch: Batch = Batch(data=pandas_animals_dataframe)
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
    pandas_animals_dataframe, expected_evr_without_unexpected_rows
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
    batch = Batch(data=pandas_animals_dataframe)
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
    pandas_animals_dataframe, expected_evr_without_unexpected_rows
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

    expectation = ExpectColumnValuesToBeInSet(expectation_configuration)
    batch = Batch(data=pandas_animals_dataframe)
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
    pandas_animals_dataframe,
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
    batch = Batch(data=pandas_animals_dataframe)
    engine = PandasExecutionEngine()
    validator = Validator(
        execution_engine=engine,
        batches=[
            batch,
        ],
    )
    with pytest.raises(ValueError):
        expectation.validate(validator)
