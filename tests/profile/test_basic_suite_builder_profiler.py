import json
import os
from collections import OrderedDict

import pytest
from freezegun import freeze_time
from numpy import Infinity

import great_expectations as ge
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource import PandasDatasource
from great_expectations.exceptions import ProfilerError
from great_expectations.profile.basic_suite_builder_profiler import (
    BasicSuiteBuilderProfiler,
)
from great_expectations.self_check.util import expectationSuiteValidationResultSchema

FALSEY_VALUES = [None, [], False]


def test__find_next_low_card_column(
    non_numeric_low_card_dataset, non_numeric_high_card_dataset
):
    columns = non_numeric_low_card_dataset.get_table_columns()
    column_cache = {}
    profiled_columns = {"numeric": [], "low_card": [], "string": [], "datetime": []}

    column = BasicSuiteBuilderProfiler._find_next_low_card_column(
        non_numeric_low_card_dataset, columns, profiled_columns, column_cache
    )
    assert column == "lowcardnonnum"
    profiled_columns["low_card"].append(column)
    assert (
        BasicSuiteBuilderProfiler._find_next_low_card_column(
            non_numeric_low_card_dataset, columns, profiled_columns, column_cache
        )
        is None
    )

    columns = non_numeric_high_card_dataset.get_table_columns()
    column_cache = {}
    profiled_columns = {"numeric": [], "low_card": [], "string": [], "datetime": []}
    assert (
        BasicSuiteBuilderProfiler._find_next_low_card_column(
            non_numeric_high_card_dataset, columns, profiled_columns, column_cache
        )
        is None
    )


def test__create_expectations_for_low_card_column(non_numeric_low_card_dataset):
    column = "lowcardnonnum"
    column_cache = {}

    expectation_suite = non_numeric_low_card_dataset.get_expectation_suite(
        suppress_warnings=True
    )
    assert len(expectation_suite.expectations) == 1

    BasicSuiteBuilderProfiler._create_expectations_for_low_card_column(
        non_numeric_low_card_dataset, column, column_cache
    )
    expectation_suite = non_numeric_low_card_dataset.get_expectation_suite(
        suppress_warnings=True
    )
    assert {
        expectation.expectation_type
        for expectation in expectation_suite.expectations
        if expectation.kwargs.get("column") == column
    } == {
        "expect_column_to_exist",
        "expect_column_distinct_values_to_be_in_set",
        "expect_column_kl_divergence_to_be_less_than",
        "expect_column_values_to_not_be_null",
    }


def test__find_next_numeric_column(
    numeric_high_card_dataset, non_numeric_low_card_dataset
):
    columns = numeric_high_card_dataset.get_table_columns()
    column_cache = {}
    profiled_columns = {"numeric": [], "low_card": [], "string": [], "datetime": []}

    column = BasicSuiteBuilderProfiler._find_next_numeric_column(
        numeric_high_card_dataset, columns, profiled_columns, column_cache
    )
    assert column == "norm_0_1"
    profiled_columns["numeric"].append(column)
    assert (
        BasicSuiteBuilderProfiler._find_next_numeric_column(
            numeric_high_card_dataset, columns, profiled_columns, column_cache
        )
        is None
    )

    columns = non_numeric_low_card_dataset.get_table_columns()
    column_cache = {}
    profiled_columns = {"numeric": [], "low_card": [], "string": [], "datetime": []}
    assert (
        BasicSuiteBuilderProfiler._find_next_numeric_column(
            non_numeric_low_card_dataset, columns, profiled_columns, column_cache
        )
        is None
    )


def test__create_expectations_for_numeric_column(
    numeric_high_card_dataset, test_backend
):
    column = "norm_0_1"

    expectation_suite = numeric_high_card_dataset.get_expectation_suite(
        suppress_warnings=True
    )
    assert len(expectation_suite.expectations) == 1

    BasicSuiteBuilderProfiler._create_expectations_for_numeric_column(
        numeric_high_card_dataset, column
    )
    expectation_suite = numeric_high_card_dataset.get_expectation_suite(
        suppress_warnings=True
    )
    if test_backend in [
        "PandasDataset",
        "SparkDFDataset",
        "postgresql",
        "mysql",
        "mssql",
    ]:
        assert {
            expectation.expectation_type
            for expectation in expectation_suite.expectations
            if expectation.kwargs.get("column") == column
        } == {
            "expect_column_to_exist",
            "expect_column_min_to_be_between",
            "expect_column_max_to_be_between",
            "expect_column_mean_to_be_between",
            "expect_column_median_to_be_between",
            "expect_column_quantile_values_to_be_between",
            "expect_column_values_to_not_be_null",
        }
    else:
        assert {
            expectation.expectation_type
            for expectation in expectation_suite.expectations
            if expectation.kwargs.get("column") == column
        } == {
            "expect_column_to_exist",
            "expect_column_min_to_be_between",
            "expect_column_max_to_be_between",
            "expect_column_mean_to_be_between",
            "expect_column_median_to_be_between",
            "expect_column_values_to_not_be_null",
        }


def test__find_next_string_column(
    non_numeric_high_card_dataset, non_numeric_low_card_dataset
):
    columns = non_numeric_high_card_dataset.get_table_columns()
    column_cache = {}
    profiled_columns = {"numeric": [], "low_card": [], "string": [], "datetime": []}

    column = BasicSuiteBuilderProfiler._find_next_string_column(
        non_numeric_high_card_dataset, columns, profiled_columns, column_cache
    )
    expected_columns = ["highcardnonnum", "medcardnonnum"]
    assert column in expected_columns
    profiled_columns["string"].append(column)
    expected_columns.remove(column)
    assert (
        BasicSuiteBuilderProfiler._find_next_string_column(
            non_numeric_high_card_dataset, columns, profiled_columns, column_cache
        )
        in expected_columns
    )

    columns = non_numeric_low_card_dataset.get_table_columns()
    column_cache = {}
    profiled_columns = {"numeric": [], "low_card": [], "string": [], "datetime": []}
    assert (
        BasicSuiteBuilderProfiler._find_next_string_column(
            non_numeric_low_card_dataset, columns, profiled_columns, column_cache
        )
        is None
    )


def test__create_expectations_for_string_column(non_numeric_high_card_dataset):
    column = "highcardnonnum"

    expectation_suite = non_numeric_high_card_dataset.get_expectation_suite(
        suppress_warnings=True
    )
    assert len(expectation_suite.expectations) == 2

    BasicSuiteBuilderProfiler._create_expectations_for_string_column(
        non_numeric_high_card_dataset, column
    )
    expectation_suite = non_numeric_high_card_dataset.get_expectation_suite(
        suppress_warnings=True
    )
    assert {
        expectation.expectation_type
        for expectation in expectation_suite.expectations
        if expectation.kwargs.get("column") == column
    } == {
        "expect_column_to_exist",
        "expect_column_values_to_not_be_null",
        "expect_column_value_lengths_to_be_between",
    }


def test__find_next_datetime_column(datetime_dataset, numeric_high_card_dataset):
    columns = datetime_dataset.get_table_columns()
    column_cache = {}
    profiled_columns = {"numeric": [], "low_card": [], "string": [], "datetime": []}

    column = BasicSuiteBuilderProfiler._find_next_datetime_column(
        datetime_dataset, columns, profiled_columns, column_cache
    )
    assert column == "datetime"
    profiled_columns["datetime"].append(column)
    assert (
        BasicSuiteBuilderProfiler._find_next_datetime_column(
            datetime_dataset, columns, profiled_columns, column_cache
        )
        is None
    )

    columns = numeric_high_card_dataset.get_table_columns()
    column_cache = {}
    profiled_columns = {"numeric": [], "low_card": [], "string": [], "datetime": []}
    assert (
        BasicSuiteBuilderProfiler._find_next_datetime_column(
            numeric_high_card_dataset, columns, profiled_columns, column_cache
        )
        is None
    )


def test__create_expectations_for_datetime_column(datetime_dataset):
    column = "datetime"

    expectation_suite = datetime_dataset.get_expectation_suite(suppress_warnings=True)
    assert len(expectation_suite.expectations) == 1

    BasicSuiteBuilderProfiler._create_expectations_for_datetime_column(
        datetime_dataset, column
    )
    expectation_suite = datetime_dataset.get_expectation_suite(suppress_warnings=True)
    assert {
        expectation.expectation_type
        for expectation in expectation_suite.expectations
        if expectation.kwargs.get("column") == column
    } == {
        "expect_column_to_exist",
        "expect_column_values_to_be_between",
        "expect_column_values_to_not_be_null",
    }


def test_BasicSuiteBuilderProfiler_raises_error_on_both_included_and_excluded_expectations(
    pandas_dataset,
):
    with pytest.raises(ProfilerError):
        BasicSuiteBuilderProfiler.profile(
            pandas_dataset,
            profiler_configuration={
                "included_expectations": ["foo"],
                "excluded_expectations": ["foo"],
            },
        )


def test_BasicSuiteBuilderProfiler_raises_error_on_both_included_and_excluded_columns(
    pandas_dataset,
):
    with pytest.raises(ProfilerError):
        BasicSuiteBuilderProfiler.profile(
            pandas_dataset,
            profiler_configuration={
                "included_columns": ["foo"],
                "excluded_columns": ["foo"],
            },
        )


@pytest.mark.skipif(os.getenv("PANDAS") == "0.22.0", reason="0.22.0 pandas")
def test_BasicSuiteBuilderProfiler_raises_error_on_non_existent_column_on_pandas(
    pandas_dataset,
):
    with pytest.raises(ProfilerError):
        BasicSuiteBuilderProfiler().profile(
            pandas_dataset,
            profiler_configuration={
                "included_columns": ["NON_EXISTENT_COLUMN"],
                "included_expectations": [
                    "expect_table_column_count_to_equal",
                    "expect_column_values_to_not_be_null",
                ],
            },
        )


def test_BasicSuiteBuilderProfiler_with_context(filesystem_csv_data_context):
    context = filesystem_csv_data_context

    context.create_expectation_suite("default")
    datasource = context.datasources["rad_datasource"]
    base_dir = datasource.config["batch_kwargs_generators"]["subdir_reader"][
        "base_directory"
    ]
    batch_kwargs = {
        "datasource": "rad_datasource",
        "path": os.path.join(base_dir, "f1.csv"),
    }
    batch = context.get_batch(batch_kwargs, "default")
    expectation_suite, validation_results = BasicSuiteBuilderProfiler.profile(
        batch, profiler_configuration="demo"
    )

    assert expectation_suite.expectation_suite_name == "default"
    assert "BasicSuiteBuilderProfiler" in expectation_suite.meta
    assert set(expectation_suite.meta["BasicSuiteBuilderProfiler"].keys()) == {
        "created_by",
        "created_at",
        "batch_kwargs",
    }
    assert (
        expectation_suite.meta["BasicSuiteBuilderProfiler"]["batch_kwargs"]
        == batch_kwargs
    )
    for exp in expectation_suite.expectations:
        assert "BasicSuiteBuilderProfiler" in exp.meta
        assert "confidence" in exp.meta["BasicSuiteBuilderProfiler"]

    assert set(validation_results.meta.keys()) == {
        "batch_kwargs",
        "batch_markers",
        "batch_parameters",
        "expectation_suite_name",
        "great_expectations_version",
        "run_id",
        "validation_time",
    }

    assert expectation_suite.meta["notes"] == {
        "format": "markdown",
        "content": [
            """#### This is an _example_ suite

- This suite was made by quickly glancing at 1000 rows of your data.
- This is **not a production suite**. It is meant to show examples of expectations.
- Because this suite was auto-generated using a very basic profiler that does not know your data like you do, many of the expectations may not be meaningful.
"""
        ],
    }

    expectation_types = [
        expectation["expectation_type"]
        for expectation in expectation_suite.expectations
    ]

    expected_expectation_types = {
        "expect_table_row_count_to_be_between",
        "expect_table_column_count_to_equal",
        "expect_table_columns_to_match_ordered_list",
        "expect_column_values_to_not_be_null",
        "expect_column_min_to_be_between",
        "expect_column_max_to_be_between",
        "expect_column_mean_to_be_between",
        "expect_column_median_to_be_between",
        "expect_column_quantile_values_to_be_between",
    }

    assert set(expectation_types) == expected_expectation_types


def test_context_profiler(filesystem_csv_data_context):
    """
    This just validates that it's possible to profile using the datasource hook,
    and have validation results available in the DataContext
    """
    context = filesystem_csv_data_context

    assert isinstance(context.datasources["rad_datasource"], PandasDatasource)
    assert context.list_expectation_suites() == []
    context.profile_datasource(
        "rad_datasource",
        profiler=BasicSuiteBuilderProfiler,
        profiler_configuration="demo",
    )

    assert len(context.list_expectation_suites()) == 1

    expected_suite_name = "rad_datasource.subdir_reader.f1.BasicSuiteBuilderProfiler"
    expectation_suite = context.get_expectation_suite(expected_suite_name)

    for exp in expectation_suite.expectations:
        assert "BasicSuiteBuilderProfiler" in exp.meta
        assert "confidence" in exp.meta["BasicSuiteBuilderProfiler"]

    assert expectation_suite.expectation_suite_name == expected_suite_name
    assert "batch_kwargs" in expectation_suite.meta["BasicSuiteBuilderProfiler"]

    assert expectation_suite.meta["notes"] == {
        "format": "markdown",
        "content": [
            """#### This is an _example_ suite

- This suite was made by quickly glancing at 1000 rows of your data.
- This is **not a production suite**. It is meant to show examples of expectations.
- Because this suite was auto-generated using a very basic profiler that does not know your data like you do, many of the expectations may not be meaningful.
"""
        ],
    }

    expectation_types = [
        expectation["expectation_type"]
        for expectation in expectation_suite.expectations
    ]

    expected_expectation_types = {
        "expect_table_row_count_to_be_between",
        "expect_table_column_count_to_equal",
        "expect_table_columns_to_match_ordered_list",
        "expect_column_values_to_not_be_null",
        "expect_column_min_to_be_between",
        "expect_column_max_to_be_between",
        "expect_column_mean_to_be_between",
        "expect_column_median_to_be_between",
        "expect_column_quantile_values_to_be_between",
    }

    assert set(expectation_types) == expected_expectation_types


@freeze_time("09/26/2019 13:42:41")
def test_snapshot_BasicSuiteBuilderProfiler_on_titanic_in_demo_mode():
    """
    A snapshot regression test for BasicSuiteBuilderProfiler.
    We are running the profiler on the Titanic dataset
    and comparing the EVRs to ones retrieved from a
    previously stored file.
    """
    df = ge.read_csv(file_relative_path(__file__, "../test_sets/Titanic.csv"))
    suite, evrs = df.profile(BasicSuiteBuilderProfiler, profiler_configuration="demo")

    # Check to make sure BasicSuiteBuilderProfiler is adding meta.columns with a single "description" field for each column
    assert "columns" in suite.meta
    for k, v in suite.meta["columns"].items():
        assert v == {"description": ""}

    # Note: the above already produces an EVR; rerunning isn't strictly necessary just for EVRs
    evrs = df.validate(result_format="SUMMARY")

    # THIS IS NOT DEAD CODE. UNCOMMENT TO SAVE A SNAPSHOT WHEN UPDATING THIS TEST
    expected_filepath = file_relative_path(
        __file__,
        "./fixtures/expected_evrs_BasicSuiteBuilderProfiler_on_titanic_demo_mode.json",
    )
    # with open(expected_filepath, 'w+') as file:
    #     json.dump(expectationSuiteValidationResultSchema.dump(evrs), file, indent=2)
    # with open(file_relative_path(__file__, '../render/fixtures/BasicSuiteBuilderProfiler_evrs.json'), 'w+') as file:
    #     json.dump(expectationSuiteValidationResultSchema.dump(evrs), file, indent=2)

    with open(
        expected_filepath,
    ) as file:
        expected_evrs = expectationSuiteValidationResultSchema.load(
            json.load(file, object_pairs_hook=OrderedDict)
        )

    # We know that python 2 does not guarantee the order of value_counts, which causes a different
    # order for items in the partial_unexpected_value_counts list
    # Remove those before assertions.
    for result in evrs.results:
        if "partial_unexpected_counts" in result.result:
            result.result.pop("partial_unexpected_counts")

    for result in expected_evrs.results:
        if "partial_unexpected_counts" in result.result:
            result.result.pop("partial_unexpected_counts")

    # Version, run_id, batch id will be different
    del expected_evrs.meta["great_expectations_version"]
    del evrs.meta["great_expectations_version"]

    del expected_evrs.meta["run_id"]
    del evrs.meta["run_id"]

    del expected_evrs.meta["batch_kwargs"]["ge_batch_id"]
    del evrs.meta["batch_kwargs"]["ge_batch_id"]

    del evrs.meta["validation_time"]

    assert evrs == expected_evrs


@pytest.mark.skipif(os.getenv("PANDAS") == "0.22.0", reason="0.22.0 pandas")
def test_BasicSuiteBuilderProfiler_uses_all_columns_if_configuration_does_not_have_included_or_excluded_columns_on_pandas(
    pandas_dataset,
):
    observed_suite, evrs = BasicSuiteBuilderProfiler().profile(pandas_dataset)
    assert isinstance(observed_suite, ExpectationSuite)

    expected = ExpectationSuite(
        "default",
        data_asset_type="Dataset",
        expectations=[
            {
                "kwargs": {"column": "infinities"},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_to_exist",
            },
            {
                "kwargs": {"column": "nulls"},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_to_exist",
            },
            {
                "kwargs": {"column": "naturals"},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_to_exist",
            },
            {
                "kwargs": {"min_value": 6, "max_value": 7},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_table_row_count_to_be_between",
            },
            {
                "kwargs": {"value": 3},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_table_column_count_to_equal",
            },
            {
                "kwargs": {"column_list": ["infinities", "nulls", "naturals"]},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_table_columns_to_match_ordered_list",
            },
            {
                "kwargs": {"column": "infinities"},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_values_to_be_unique",
            },
            {
                "kwargs": {"column": "infinities"},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_values_to_not_be_null",
            },
            {
                "kwargs": {
                    "column": "infinities",
                    "min_value": -Infinity,
                    "max_value": -Infinity,
                },
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_min_to_be_between",
            },
            {
                "kwargs": {
                    "column": "infinities",
                    "min_value": Infinity,
                    "max_value": Infinity,
                },
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_max_to_be_between",
            },
            {
                "kwargs": {
                    "column": "infinities",
                    "min_value": None,
                    "max_value": None,
                },
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_mean_to_be_between",
            },
            {
                "kwargs": {"column": "infinities", "min_value": -1.0, "max_value": 1.0},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_median_to_be_between",
            },
            {
                "kwargs": {
                    "column": "infinities",
                    "quantile_ranges": {
                        "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                        "value_ranges": [
                            [-Infinity, -Infinity],
                            [-4.141592653589793, -2.141592653589793],
                            [-1.0, 1.0],
                            [2.141592653589793, 4.141592653589793],
                            [Infinity, Infinity],
                        ],
                    },
                    "allow_relative_error": False,
                },
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_quantile_values_to_be_between",
            },
            {
                "kwargs": {"column": "nulls"},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_values_to_be_unique",
            },
            {
                "kwargs": {"column": "nulls", "mostly": 0.471},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_values_to_not_be_null",
            },
            {
                "kwargs": {"column": "nulls", "min_value": -1.0, "max_value": 1.0},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_min_to_be_between",
            },
            {
                "kwargs": {"column": "nulls", "min_value": 2.3, "max_value": 4.3},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_max_to_be_between",
            },
            {
                "kwargs": {
                    "column": "nulls",
                    "min_value": 0.6499999999999999,
                    "max_value": 2.65,
                },
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_mean_to_be_between",
            },
            {
                "kwargs": {
                    "column": "nulls",
                    "min_value": 0.6500000000000001,
                    "max_value": 2.6500000000000004,
                },
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_median_to_be_between",
            },
            {
                "kwargs": {
                    "column": "nulls",
                    "quantile_ranges": {
                        "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                        "value_ranges": [
                            [-1.0, 1.0],
                            [0.10000000000000009, 2.1],
                            [1.2000000000000002, 3.2],
                            [1.2000000000000002, 3.2],
                            [2.3, 4.3],
                        ],
                    },
                    "allow_relative_error": False,
                },
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_quantile_values_to_be_between",
            },
            {
                "kwargs": {"column": "naturals"},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_values_to_be_unique",
            },
            {
                "kwargs": {"column": "naturals"},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_values_to_not_be_null",
            },
            {
                "kwargs": {"column": "naturals", "min_value": 0.0, "max_value": 2.0},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_min_to_be_between",
            },
            {
                "kwargs": {"column": "naturals", "min_value": 6.0, "max_value": 8.0},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_max_to_be_between",
            },
            {
                "kwargs": {"column": "naturals", "min_value": 3.0, "max_value": 5.0},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_mean_to_be_between",
            },
            {
                "kwargs": {"column": "naturals", "min_value": 3.0, "max_value": 5.0},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_median_to_be_between",
            },
            {
                "kwargs": {
                    "column": "naturals",
                    "quantile_ranges": {
                        "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                        "value_ranges": [
                            [0.0, 2.0],
                            [2.0, 4.0],
                            [3.0, 5.0],
                            [4.0, 6.0],
                            [6.0, 8.0],
                        ],
                    },
                    "allow_relative_error": False,
                },
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_quantile_values_to_be_between",
            },
        ],
    )

    # remove metadata to simplify assertions
    observed_suite.meta = None
    expected.meta = None
    assert observed_suite == expected


@pytest.mark.skipif(os.getenv("PANDAS") == "0.22.0", reason="0.22.0 pandas")
def test_BasicSuiteBuilderProfiler_uses_selected_columns_on_pandas(
    pandas_dataset,
):
    observed_suite, evrs = BasicSuiteBuilderProfiler().profile(
        pandas_dataset, profiler_configuration={"included_columns": ["naturals"]}
    )
    assert isinstance(observed_suite, ExpectationSuite)

    expected = ExpectationSuite(
        "default",
        data_asset_type="Dataset",
        expectations=[
            {
                "kwargs": {"column": "infinities"},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_to_exist",
            },
            {
                "kwargs": {"column": "nulls"},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_to_exist",
            },
            {
                "kwargs": {"column": "naturals"},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_to_exist",
            },
            {
                "kwargs": {"min_value": 6, "max_value": 7},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_table_row_count_to_be_between",
            },
            {
                "kwargs": {"value": 3},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_table_column_count_to_equal",
            },
            {
                "kwargs": {"column_list": ["infinities", "nulls", "naturals"]},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_table_columns_to_match_ordered_list",
            },
            {
                "kwargs": {"column": "naturals"},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_values_to_be_unique",
            },
            {
                "kwargs": {"column": "naturals"},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_values_to_not_be_null",
            },
            {
                "kwargs": {"column": "naturals", "min_value": 0.0, "max_value": 2.0},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_min_to_be_between",
            },
            {
                "kwargs": {"column": "naturals", "min_value": 6.0, "max_value": 8.0},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_max_to_be_between",
            },
            {
                "kwargs": {"column": "naturals", "min_value": 3.0, "max_value": 5.0},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_mean_to_be_between",
            },
            {
                "kwargs": {"column": "naturals", "min_value": 3.0, "max_value": 5.0},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_median_to_be_between",
            },
            {
                "kwargs": {
                    "column": "naturals",
                    "quantile_ranges": {
                        "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                        "value_ranges": [
                            [0.0, 2.0],
                            [2.0, 4.0],
                            [3.0, 5.0],
                            [4.0, 6.0],
                            [6.0, 8.0],
                        ],
                    },
                    "allow_relative_error": False,
                },
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_quantile_values_to_be_between",
            },
        ],
    )

    # remove metadata to simplify assertions
    observed_suite.meta = None
    expected.meta = None
    assert observed_suite == expected


@pytest.mark.skipif(os.getenv("PANDAS") == "0.22.0", reason="0.22.0 pandas")
def test_BasicSuiteBuilderProfiler_respects_excluded_expectations_on_pandas(
    pandas_dataset,
):
    observed_suite, evrs = BasicSuiteBuilderProfiler().profile(
        pandas_dataset,
        profiler_configuration={
            "included_columns": ["naturals"],
            "excluded_expectations": [
                "expect_table_column_count_to_equal",
                "expect_column_values_to_be_unique",
                "expect_column_min_to_be_between",
                "expect_column_max_to_be_between",
                "expect_column_mean_to_be_between",
                "expect_column_median_to_be_between",
                "expect_column_quantile_values_to_be_between",
            ],
        },
    )
    assert isinstance(observed_suite, ExpectationSuite)

    expected = ExpectationSuite(
        "default",
        data_asset_type="Dataset",
        expectations=[
            {
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "kwargs": {"column": "infinities"},
                "expectation_type": "expect_column_to_exist",
            },
            {
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "kwargs": {"column": "nulls"},
                "expectation_type": "expect_column_to_exist",
            },
            {
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "kwargs": {"column": "naturals"},
                "expectation_type": "expect_column_to_exist",
            },
            {
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "kwargs": {"min_value": 6, "max_value": 7},
                "expectation_type": "expect_table_row_count_to_be_between",
            },
            {
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "kwargs": {"column_list": ["infinities", "nulls", "naturals"]},
                "expectation_type": "expect_table_columns_to_match_ordered_list",
            },
            {
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "kwargs": {"column": "naturals"},
                "expectation_type": "expect_column_values_to_not_be_null",
            },
        ],
    )

    # remove metadata to simplify assertions
    observed_suite.meta = None
    expected.meta = None
    assert observed_suite == expected


@pytest.mark.skipif(os.getenv("PANDAS") == "0.22.0", reason="0.22.0 pandas")
def test_BasicSuiteBuilderProfiler_respects_included_expectations_on_pandas(
    pandas_dataset,
):
    observed_suite, evrs = BasicSuiteBuilderProfiler().profile(
        pandas_dataset,
        profiler_configuration={
            "included_columns": ["naturals", "nulls"],
            "included_expectations": [
                "expect_table_column_count_to_equal",
                "expect_column_values_to_not_be_null",
            ],
        },
    )
    assert isinstance(observed_suite, ExpectationSuite)

    expected = ExpectationSuite(
        "default",
        data_asset_type="Dataset",
        expectations=[
            {
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "kwargs": {"value": 3},
                "expectation_type": "expect_table_column_count_to_equal",
            },
            {
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "kwargs": {"column": "naturals"},
                "expectation_type": "expect_column_values_to_not_be_null",
            },
            {
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "kwargs": {"column": "nulls", "mostly": 0.471},
                "expectation_type": "expect_column_values_to_not_be_null",
            },
        ],
    )

    # remove metadata to simplify assertions
    observed_suite.meta = None
    expected.meta = None
    assert observed_suite == expected


@pytest.mark.skipif(os.getenv("PANDAS") == "0.22.0", reason="0.22.0 pandas")
@pytest.mark.parametrize("included_columns", FALSEY_VALUES)
def test_BasicSuiteBuilderProfiler_uses_no_columns_if_included_columns_are_falsey_on_pandas(
    included_columns,
    pandas_dataset,
):
    observed_suite, evrs = BasicSuiteBuilderProfiler().profile(
        pandas_dataset,
        profiler_configuration={
            "included_columns": included_columns,
            "included_expectations": [
                "expect_table_column_count_to_equal",
                "expect_column_values_to_not_be_null",
            ],
        },
    )
    assert isinstance(observed_suite, ExpectationSuite)

    expected = ExpectationSuite(
        "default",
        data_asset_type="Dataset",
        expectations=[
            {
                "expectation_type": "expect_table_column_count_to_equal",
                "kwargs": {"value": 3},
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
        ],
    )

    # remove metadata to simplify assertions
    observed_suite.meta = None
    expected.meta = None
    assert observed_suite == expected


@pytest.mark.skipif(os.getenv("PANDAS") == "0.22.0", reason="0.22.0 pandas")
@pytest.mark.parametrize("included_expectations", FALSEY_VALUES)
def test_BasicSuiteBuilderProfiler_uses_no_expectations_if_included_expectations_are_falsey_on_pandas(
    included_expectations,
    pandas_dataset,
):
    observed_suite, evrs = BasicSuiteBuilderProfiler().profile(
        pandas_dataset,
        profiler_configuration={
            "included_expectations": included_expectations,
        },
    )
    assert isinstance(observed_suite, ExpectationSuite)

    expected = ExpectationSuite(
        "default",
        data_asset_type="Dataset",
        expectations=[],
    )

    # remove metadata to simplify assertions
    observed_suite.meta = None
    expected.meta = None
    assert observed_suite == expected


@pytest.mark.skipif(os.getenv("PANDAS") == "0.22.0", reason="0.22.0 pandas")
@pytest.mark.parametrize("excluded_expectations", FALSEY_VALUES)
def test_BasicSuiteBuilderProfiler_uses_all_expectations_if_excluded_expectations_are_falsey_on_pandas(
    excluded_expectations,
    pandas_dataset,
):
    observed_suite, evrs = BasicSuiteBuilderProfiler().profile(
        pandas_dataset,
        profiler_configuration={
            "excluded_expectations": excluded_expectations,
        },
    )
    assert isinstance(observed_suite, ExpectationSuite)

    expected = ExpectationSuite(
        "default",
        data_asset_type="Dataset",
        expectations=[
            {
                "kwargs": {"column": "infinities"},
                "expectation_type": "expect_column_to_exist",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {"column": "nulls"},
                "expectation_type": "expect_column_to_exist",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {"column": "naturals"},
                "expectation_type": "expect_column_to_exist",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {"min_value": 6, "max_value": 7},
                "expectation_type": "expect_table_row_count_to_be_between",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {"value": 3},
                "expectation_type": "expect_table_column_count_to_equal",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {"column_list": ["infinities", "nulls", "naturals"]},
                "expectation_type": "expect_table_columns_to_match_ordered_list",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {"column": "infinities"},
                "expectation_type": "expect_column_values_to_be_unique",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {"column": "infinities"},
                "expectation_type": "expect_column_values_to_not_be_null",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {
                    "column": "infinities",
                    "min_value": -Infinity,
                    "max_value": -Infinity,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {
                    "column": "infinities",
                    "min_value": Infinity,
                    "max_value": Infinity,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {
                    "column": "infinities",
                    "min_value": None,
                    "max_value": None,
                },
                "expectation_type": "expect_column_mean_to_be_between",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {"column": "infinities", "min_value": -1.0, "max_value": 1.0},
                "expectation_type": "expect_column_median_to_be_between",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {
                    "column": "infinities",
                    "quantile_ranges": {
                        "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                        "value_ranges": [
                            [-Infinity, -Infinity],
                            [-4.141592653589793, -2.141592653589793],
                            [-1.0, 1.0],
                            [2.141592653589793, 4.141592653589793],
                            [Infinity, Infinity],
                        ],
                    },
                    "allow_relative_error": False,
                },
                "expectation_type": "expect_column_quantile_values_to_be_between",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {"column": "nulls"},
                "expectation_type": "expect_column_values_to_be_unique",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {"column": "nulls", "mostly": 0.471},
                "expectation_type": "expect_column_values_to_not_be_null",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {"column": "nulls", "min_value": -1.0, "max_value": 1.0},
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {"column": "nulls", "min_value": 2.3, "max_value": 4.3},
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {
                    "column": "nulls",
                    "min_value": 0.6499999999999999,
                    "max_value": 2.65,
                },
                "expectation_type": "expect_column_mean_to_be_between",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {
                    "column": "nulls",
                    "min_value": 0.6500000000000001,
                    "max_value": 2.6500000000000004,
                },
                "expectation_type": "expect_column_median_to_be_between",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {
                    "column": "nulls",
                    "quantile_ranges": {
                        "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                        "value_ranges": [
                            [-1.0, 1.0],
                            [0.10000000000000009, 2.1],
                            [1.2000000000000002, 3.2],
                            [1.2000000000000002, 3.2],
                            [2.3, 4.3],
                        ],
                    },
                    "allow_relative_error": False,
                },
                "expectation_type": "expect_column_quantile_values_to_be_between",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {"column": "naturals"},
                "expectation_type": "expect_column_values_to_be_unique",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {"column": "naturals"},
                "expectation_type": "expect_column_values_to_not_be_null",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {"column": "naturals", "min_value": 0.0, "max_value": 2.0},
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {"column": "naturals", "min_value": 6.0, "max_value": 8.0},
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {"column": "naturals", "min_value": 3.0, "max_value": 5.0},
                "expectation_type": "expect_column_mean_to_be_between",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {"column": "naturals", "min_value": 3.0, "max_value": 5.0},
                "expectation_type": "expect_column_median_to_be_between",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "kwargs": {
                    "column": "naturals",
                    "quantile_ranges": {
                        "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                        "value_ranges": [
                            [0.0, 2.0],
                            [2.0, 4.0],
                            [3.0, 5.0],
                            [4.0, 6.0],
                            [6.0, 8.0],
                        ],
                    },
                    "allow_relative_error": False,
                },
                "expectation_type": "expect_column_quantile_values_to_be_between",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
            },
        ],
    )

    # remove metadata to simplify assertions
    observed_suite.meta = None
    expected.meta = None
    assert observed_suite == expected


@pytest.mark.skipif(os.getenv("PANDAS") == "0.22.0", reason="0.22.0 pandas")
@pytest.mark.parametrize("excluded_columns", FALSEY_VALUES)
def test_BasicSuiteBuilderProfiler_uses_all_columns_if_excluded_columns_are_falsey_on_pandas(
    excluded_columns,
    pandas_dataset,
):
    observed_suite, evrs = BasicSuiteBuilderProfiler().profile(
        pandas_dataset,
        profiler_configuration={
            "excluded_columns": excluded_columns,
            "included_expectations": [
                "expect_table_column_count_to_equal",
                "expect_column_values_to_not_be_null",
            ],
        },
    )
    assert isinstance(observed_suite, ExpectationSuite)

    expected = ExpectationSuite(
        "default",
        data_asset_type="Dataset",
        expectations=[
            {
                "expectation_type": "expect_table_column_count_to_equal",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "kwargs": {"value": 3},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "kwargs": {"column": "naturals"},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "kwargs": {"column": "infinities"},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "meta": {"BasicSuiteBuilderProfiler": {"confidence": "very low"}},
                "kwargs": {"column": "nulls", "mostly": 0.471},
            },
        ],
    )

    # remove metadata to simplify assertions
    observed_suite.meta = None
    expected.meta = None
    assert len(observed_suite.expectations) == 4
    for ee in expected.expectations:
        assert ee in observed_suite.expectations
    # assert observed_suite == expected


@pytest.mark.skipif(os.getenv("PANDAS") == "0.22.0", reason="0.22.0 pandas")
def test_BasicSuiteBuilderProfiler_raises_error_on_not_real_expectations_in_included_expectations_on_pandas(
    pandas_dataset,
):
    with pytest.raises(ProfilerError):
        BasicSuiteBuilderProfiler().profile(
            pandas_dataset,
            profiler_configuration={
                "included_expectations": [
                    "expect_table_column_count_to_equal",
                    "expect_this_to_not_be_a_real_expectation",
                    "foo",
                ],
            },
        )


@pytest.mark.skipif(os.getenv("PANDAS") == "0.22.0", reason="0.22.0 pandas")
def test_BasicSuiteBuilderProfiler_raises_error_on_not_real_expectations_in_excluded_expectations_on_pandas(
    pandas_dataset,
):
    with pytest.raises(ProfilerError):
        BasicSuiteBuilderProfiler().profile(
            pandas_dataset,
            profiler_configuration={
                "included_expectations": [
                    "expect_table_column_count_to_equal",
                    "expect_this_to_not_be_a_real_expectation",
                    "foo",
                ],
            },
        )


@freeze_time("09/26/2019 13:42:41")
def test_snapshot_BasicSuiteBuilderProfiler_on_titanic_with_builder_configuration():
    """
    A snapshot regression test for BasicSuiteBuilderProfiler.

    We are running the profiler on the Titanic dataset and comparing the EVRs
    to ones retrieved from a previously stored file.
    """
    batch = ge.read_csv(file_relative_path(__file__, "../test_sets/Titanic.csv"))
    suite, evrs = BasicSuiteBuilderProfiler().profile(
        batch,
        profiler_configuration={
            "included_columns": ["Name", "PClass", "Age", "Sex", "SexCode"]
        },
    )

    # Check to make sure SuiteBuilderProfiler is adding meta.columns with a single "description" field for each column
    assert "columns" in suite.meta
    for k, v in suite.meta["columns"].items():
        assert v == {"description": ""}

    # Note: the above already produces an EVR; rerunning isn't strictly necessary just for EVRs
    evrs = batch.validate(result_format="SUMMARY")

    expected_filepath = file_relative_path(
        __file__,
        "./fixtures/expected_evrs_SuiteBuilderProfiler_on_titanic_with_configurations.json",
    )
    # THIS IS NOT DEAD CODE. UNCOMMENT TO SAVE A SNAPSHOT WHEN UPDATING THIS TEST
    # with open(expected_filepath, 'w+') as file:
    #     json.dump(expectationSuiteValidationResultSchema.dump(evrs), file, indent=2)
    # with open(file_relative_path(__file__, '../render/fixtures/SuiteBuilderProfiler_evrs.json'), 'w+') as file:
    #     json.dump(expectationSuiteValidationResultSchema.dump(evrs), file, indent=2)

    with open(
        expected_filepath,
    ) as file:
        expected_evrs = expectationSuiteValidationResultSchema.load(
            json.load(file, object_pairs_hook=OrderedDict)
        )

        # Version and RUN-ID will be different
    del expected_evrs.meta["great_expectations_version"]
    del evrs.meta["great_expectations_version"]
    del expected_evrs.meta["run_id"]
    del expected_evrs.meta["batch_kwargs"]["ge_batch_id"]
    del evrs.meta["run_id"]
    del evrs.meta["batch_kwargs"]["ge_batch_id"]
    del evrs.meta["validation_time"]

    assert evrs == expected_evrs
