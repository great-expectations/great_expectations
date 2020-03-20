import json
import os
from collections import OrderedDict

import pytest
from six import PY2

import great_expectations as ge
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource import PandasDatasource
from great_expectations.profile.sample_expectations_dataset_profiler import \
    SampleExpectationsDatasetProfiler
from tests.test_utils import expectationSuiteValidationResultSchema


@pytest.fixture()
def not_empty_datacontext(empty_data_context, filesystem_csv_2):
    empty_data_context.add_datasource(
        "rad_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": str(filesystem_csv_2),
            }
        },
    )
    return empty_data_context


def test__find_next_low_card_column(non_numeric_low_card_dataset, non_numeric_high_card_dataset):
    columns = non_numeric_low_card_dataset.get_table_columns()
    column_cache = {}
    profiled_columns = {
        "numeric": [],
        "low_card": [],
        "string": [],
        "datetime": []
    }

    column = SampleExpectationsDatasetProfiler._find_next_low_card_column(
        non_numeric_low_card_dataset,
        columns,
        profiled_columns,
        column_cache
    )
    assert column == "lowcardnonnum"
    profiled_columns["low_card"].append(column)
    assert SampleExpectationsDatasetProfiler._find_next_low_card_column(
        non_numeric_low_card_dataset,
        columns,
        profiled_columns,
        column_cache
    ) is None

    columns = non_numeric_high_card_dataset.get_table_columns()
    column_cache = {}
    profiled_columns = {
        "numeric": [],
        "low_card": [],
        "string": [],
        "datetime": []
    }
    assert SampleExpectationsDatasetProfiler._find_next_low_card_column(
        non_numeric_high_card_dataset,
        columns,
        profiled_columns,
        column_cache
    ) is None


def test__create_expectations_for_low_card_column(non_numeric_low_card_dataset):
    column = "lowcardnonnum"
    column_cache = {}

    expectation_suite = non_numeric_low_card_dataset.get_expectation_suite(suppress_warnings=True)
    assert len(expectation_suite.expectations) == 1

    SampleExpectationsDatasetProfiler._create_expectations_for_low_card_column(
        non_numeric_low_card_dataset,
        column,
        column_cache
    )
    expectation_suite = non_numeric_low_card_dataset.get_expectation_suite(suppress_warnings=True)
    assert set(
        [
            expectation.expectation_type
            for expectation in expectation_suite.expectations
            if expectation.kwargs.get("column") == column
        ]
    ) == {
            "expect_column_to_exist",
            'expect_column_distinct_values_to_be_in_set',
            "expect_column_kl_divergence_to_be_less_than",
            "expect_column_values_to_not_be_null",
    }


def test__find_next_numeric_column(numeric_high_card_dataset, non_numeric_low_card_dataset):
    columns = numeric_high_card_dataset.get_table_columns()
    column_cache = {}
    profiled_columns = {
        "numeric": [],
        "low_card": [],
        "string": [],
        "datetime": []
    }

    column = SampleExpectationsDatasetProfiler._find_next_numeric_column(
        numeric_high_card_dataset,
        columns,
        profiled_columns,
        column_cache
    )
    assert column == "norm_0_1"
    profiled_columns["numeric"].append(column)
    assert SampleExpectationsDatasetProfiler._find_next_numeric_column(
        numeric_high_card_dataset,
        columns,
        profiled_columns,
        column_cache
    ) is None

    columns = non_numeric_low_card_dataset.get_table_columns()
    column_cache = {}
    profiled_columns = {
        "numeric": [],
        "low_card": [],
        "string": [],
        "datetime": []
    }
    assert SampleExpectationsDatasetProfiler._find_next_numeric_column(
        non_numeric_low_card_dataset,
        columns,
        profiled_columns,
        column_cache
    ) is None


def test__create_expectations_for_numeric_column(numeric_high_card_dataset, test_backend):
    column = "norm_0_1"

    expectation_suite = numeric_high_card_dataset.get_expectation_suite(suppress_warnings=True)
    assert len(expectation_suite.expectations) == 1

    SampleExpectationsDatasetProfiler._create_expectations_for_numeric_column(
        numeric_high_card_dataset,
        column
    )
    expectation_suite = numeric_high_card_dataset.get_expectation_suite(suppress_warnings=True)
    if test_backend in ["PandasDataset", "SparkDFDataset", "postgresql"]:
        assert set(
            [
                expectation.expectation_type
                for expectation in expectation_suite.expectations
                if expectation.kwargs.get("column") == column
            ]
        ) == {
            "expect_column_to_exist",
            "expect_column_min_to_be_between",
            "expect_column_max_to_be_between",
            "expect_column_mean_to_be_between",
            "expect_column_median_to_be_between",
            "expect_column_quantile_values_to_be_between",
            "expect_column_values_to_not_be_null"
        }
    else:
        assert set(
            [
                expectation.expectation_type
                for expectation in expectation_suite.expectations
                if expectation.kwargs.get("column") == column
            ]
        ) == {
                   "expect_column_to_exist",
                   "expect_column_min_to_be_between",
                   "expect_column_max_to_be_between",
                   "expect_column_mean_to_be_between",
                   "expect_column_median_to_be_between",
                   "expect_column_values_to_not_be_null"
               }


def test__find_next_string_column(non_numeric_high_card_dataset, non_numeric_low_card_dataset):
    columns = non_numeric_high_card_dataset.get_table_columns()
    column_cache = {}
    profiled_columns = {
        "numeric": [],
        "low_card": [],
        "string": [],
        "datetime": []
    }

    column = SampleExpectationsDatasetProfiler._find_next_string_column(
        non_numeric_high_card_dataset,
        columns,
        profiled_columns,
        column_cache
    )
    expected_columns = ["highcardnonnum", "medcardnonnum"]
    assert column in expected_columns
    profiled_columns["string"].append(column)
    expected_columns.remove(column)
    assert SampleExpectationsDatasetProfiler._find_next_string_column(
        non_numeric_high_card_dataset,
        columns,
        profiled_columns,
        column_cache
    ) in expected_columns

    columns = non_numeric_low_card_dataset.get_table_columns()
    column_cache = {}
    profiled_columns = {
        "numeric": [],
        "low_card": [],
        "string": [],
        "datetime": []
    }
    assert SampleExpectationsDatasetProfiler._find_next_string_column(
        non_numeric_low_card_dataset,
        columns,
        profiled_columns,
        column_cache
    ) is None


def test__create_expectations_for_string_column(non_numeric_high_card_dataset):
    column = "highcardnonnum"

    expectation_suite = non_numeric_high_card_dataset.get_expectation_suite(suppress_warnings=True)
    assert len(expectation_suite.expectations) == 2

    SampleExpectationsDatasetProfiler._create_expectations_for_string_column(
        non_numeric_high_card_dataset,
        column
    )
    expectation_suite = non_numeric_high_card_dataset.get_expectation_suite(suppress_warnings=True)
    assert set(
        [
            expectation.expectation_type
            for expectation in expectation_suite.expectations
            if expectation.kwargs.get("column") == column
        ]
    ) == {
        "expect_column_to_exist",
        "expect_column_values_to_not_be_null",
        "expect_column_value_lengths_to_be_between"
    }


def test__find_next_datetime_column(datetime_dataset, numeric_high_card_dataset):
    columns = datetime_dataset.get_table_columns()
    column_cache = {}
    profiled_columns = {
        "numeric": [],
        "low_card": [],
        "string": [],
        "datetime": []
    }

    column = SampleExpectationsDatasetProfiler._find_next_datetime_column(
        datetime_dataset,
        columns,
        profiled_columns,
        column_cache
    )
    assert column == "datetime"
    profiled_columns["datetime"].append(column)
    assert SampleExpectationsDatasetProfiler._find_next_datetime_column(
        datetime_dataset,
        columns,
        profiled_columns,
        column_cache
    ) is None

    columns = numeric_high_card_dataset.get_table_columns()
    column_cache = {}
    profiled_columns = {
        "numeric": [],
        "low_card": [],
        "string": [],
        "datetime": []
    }
    assert SampleExpectationsDatasetProfiler._find_next_datetime_column(
        numeric_high_card_dataset,
        columns,
        profiled_columns,
        column_cache
    ) is None


def test__create_expectations_for_datetime_column(datetime_dataset):
    column = "datetime"

    expectation_suite = datetime_dataset.get_expectation_suite(suppress_warnings=True)
    assert len(expectation_suite.expectations) == 1

    SampleExpectationsDatasetProfiler._create_expectations_for_datetime_column(
        datetime_dataset,
        column
    )
    expectation_suite = datetime_dataset.get_expectation_suite(suppress_warnings=True)
    assert set(
        [
            expectation.expectation_type
            for expectation in expectation_suite.expectations
            if expectation.kwargs.get("column") == column
        ]
    ) == {
        "expect_column_to_exist",
        "expect_column_values_to_be_between",
        "expect_column_values_to_not_be_null"
    }


def test_SampleExpectationsDatasetProfiler_with_context(not_empty_datacontext):
    context = not_empty_datacontext

    context.create_expectation_suite("default")
    datasource = context.datasources["rad_datasource"]
    base_dir = datasource.config["generators"]["subdir_reader"]["base_directory"]
    batch_kwargs = {
        "datasource": "rad_datasource",
        "path": os.path.join(base_dir, "f1.csv"),
    }
    batch = context.get_batch(batch_kwargs, "default")
    expectation_suite, validation_results = SampleExpectationsDatasetProfiler.profile(batch)

    assert expectation_suite.expectation_suite_name == "default"
    assert "SampleExpectationsDatasetProfiler" in expectation_suite.meta
    assert set(expectation_suite.meta["SampleExpectationsDatasetProfiler"].keys()) == {
        "created_by",
        "created_at",
        "batch_kwargs",
    }
    assert (
            expectation_suite.meta["SampleExpectationsDatasetProfiler"]["batch_kwargs"] == batch_kwargs
    )
    for exp in expectation_suite.expectations:
        assert "SampleExpectationsDatasetProfiler" in exp.meta
        assert "confidence" in exp.meta["SampleExpectationsDatasetProfiler"]

    assert set(validation_results.meta.keys()) == {
        "batch_kwargs",
        "batch_markers",
        "batch_parameters",
        "expectation_suite_name",
        "great_expectations.__version__",
        "run_id",
    }

    assert expectation_suite.meta["notes"] == {
        "format": "markdown",
        "content": [
            """#### This is an _example_ suite

- This suite was made by quickly glancing at 1000 rows of your data.
- This is **not a production suite**. It is meant to show examples of expectations.
- Because this suite was auto-generated using a very basic profiler that does not know your data like you do, many of the expectations may not be meaningful.
"""
        ]
    }

    expectation_types = [expectation["expectation_type"] for expectation in expectation_suite.expectations]

    expected_expectation_types = {'expect_table_row_count_to_be_between', 'expect_table_column_count_to_equal',
                                  'expect_table_columns_to_match_ordered_list', 'expect_column_values_to_not_be_null',
                                  'expect_column_min_to_be_between', 'expect_column_max_to_be_between',
                                  'expect_column_mean_to_be_between', 'expect_column_median_to_be_between',
                                  'expect_column_quantile_values_to_be_between'}

    assert set(expectation_types) == expected_expectation_types


def test_context_profiler(not_empty_datacontext):
    """
    This just validates that it's possible to profile using the datasource hook,
    and have validation results available in the DataContext
    """
    context = not_empty_datacontext

    assert isinstance(context.datasources["rad_datasource"], PandasDatasource)
    assert context.list_expectation_suites() == []
    context.profile_datasource("rad_datasource", profiler=SampleExpectationsDatasetProfiler)

    assert len(context.list_expectation_suites()) == 1

    expected_suite_name = "rad_datasource.subdir_reader.f1.SampleExpectationsDatasetProfiler"
    expectation_suite = context.get_expectation_suite(expected_suite_name)

    for exp in expectation_suite.expectations:
        assert "SampleExpectationsDatasetProfiler" in exp.meta
        assert "confidence" in exp.meta["SampleExpectationsDatasetProfiler"]

    assert expectation_suite.expectation_suite_name == expected_suite_name
    assert "batch_kwargs" in expectation_suite.meta["SampleExpectationsDatasetProfiler"]

    assert expectation_suite.meta["notes"] == {
        "format": "markdown",
        "content": [
            """#### This is an _example_ suite

- This suite was made by quickly glancing at 1000 rows of your data.
- This is **not a production suite**. It is meant to show examples of expectations.
- Because this suite was auto-generated using a very basic profiler that does not know your data like you do, many of the expectations may not be meaningful.
"""
        ]
    }

    expectation_types = [expectation["expectation_type"] for expectation in expectation_suite.expectations]

    expected_expectation_types = {'expect_table_row_count_to_be_between', 'expect_table_column_count_to_equal',
                                  'expect_table_columns_to_match_ordered_list', 'expect_column_values_to_not_be_null',
                                  'expect_column_min_to_be_between', 'expect_column_max_to_be_between',
                                  'expect_column_mean_to_be_between', 'expect_column_median_to_be_between',
                                  'expect_column_quantile_values_to_be_between'}

    assert set(expectation_types) == expected_expectation_types


def test_snapshot_SampleExpectationsDatasetProfiler_on_titanic():
    """
    A snapshot regression test for SampleExpectationsDatasetProfiler.
    We are running the profiler on the Titanic dataset
    and comparing the EVRs to ones retrieved from a
    previously stored file.
    """
    df = ge.read_csv(file_relative_path(__file__, "../test_sets/Titanic.csv"))
    suite, evrs = df.profile(SampleExpectationsDatasetProfiler)

    # Check to make sure SampleExpectationsDatasetProfiler is adding meta.columns with a single "description" field for each column
    assert "columns" in suite.meta
    for k, v in suite.meta["columns"].items():
        assert v == {"description": ""}

    # Note: the above already produces an EVR; rerunning isn't strictly necessary just for EVRs
    evrs = df.validate(result_format="SUMMARY")

    # THIS IS NOT DEAD CODE. UNCOMMENT TO SAVE A SNAPSHOT WHEN UPDATING THIS TEST
    # with open(file_relative_path(__file__, '../test_sets/expected_evrs_SampleExpectationsDatasetProfiler_on_titanic.json'), 'w+') as file:
    #     json.dump(expectationSuiteValidationResultSchema.dump(evrs), file, indent=2)
    # with open(file_relative_path(__file__, '../render/fixtures/SampleExpectationsDatasetProfiler_evrs.json'), 'w+') as file:
    #     json.dump(expectationSuiteValidationResultSchema.dump(evrs), file, indent=2)

    with open(
            file_relative_path(
                __file__, "../test_sets/expected_evrs_SampleExpectationsDatasetProfiler_on_titanic.json"
            ),
            "r",
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

    # Version and RUN-ID will be different
    del expected_evrs.meta["great_expectations.__version__"]
    del evrs.meta["great_expectations.__version__"]
    del expected_evrs.meta["run_id"]
    del expected_evrs.meta["batch_kwargs"]["ge_batch_id"]
    del evrs.meta["run_id"]
    del evrs.meta["batch_kwargs"]["ge_batch_id"]

    # DISABLE TEST IN PY2 BECAUSE OF ORDER ISSUE AND NEAR-EOL
    if not PY2:
        assert expected_evrs == evrs