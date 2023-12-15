import os
from unittest import mock

import pytest

from great_expectations.dataset.pandas_dataset import PandasDataset
from great_expectations.profile.base import DatasetProfiler, Profiler
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.profile.columns_exist import ColumnsExistProfiler


@pytest.mark.unit
def test_base_class_not_instantiable_due_to_abstract_methods():
    with pytest.raises(TypeError):
        Profiler()


@pytest.mark.unit
def test_DataSetProfiler_methods():
    toy_dataset = PandasDataset({"x": [1, 2, 3]})

    assert DatasetProfiler.validate(1) is False
    assert DatasetProfiler.validate(toy_dataset)

    with pytest.raises(NotImplementedError):
        DatasetProfiler.profile(toy_dataset)


@pytest.mark.unit
def test_ColumnsExistProfiler():
    toy_dataset = PandasDataset({"x": [1, 2, 3]})

    expectations_config, evr_config = ColumnsExistProfiler.profile(toy_dataset)

    assert len(expectations_config.expectations) == 1
    assert (
        expectations_config.expectation_configurations[0].expectation_type
        == "expect_column_to_exist"
    )
    assert expectations_config.expectation_configurations[0].kwargs["column"] == "x"


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@pytest.mark.unit
def test_BasicDatasetProfiler(mock_emit):
    toy_dataset = PandasDataset(
        {"x": [1, 2, 3]},
    )
    assert (
        len(
            toy_dataset.get_expectation_suite(
                suppress_warnings=True
            ).expectation_configurations
        )
        == 0
    )

    expectations_config, evr_config = BasicDatasetProfiler.profile(toy_dataset)

    assert (
        len(toy_dataset.get_expectation_suite(suppress_warnings=True).expectations) > 0
    )

    assert "BasicDatasetProfiler" in expectations_config.meta

    assert set(expectations_config.meta["BasicDatasetProfiler"].keys()) == {
        "created_by",
        "created_at",
        "batch_kwargs",
    }

    assert "notes" in expectations_config.meta
    assert set(expectations_config.meta["notes"].keys()) == {"format", "content"}
    assert "To add additional notes" in expectations_config.meta["notes"]["content"][0]

    added_expectations = set()
    for exp in expectations_config.expectation_configurations:
        added_expectations.add(exp.expectation_type)
        assert "BasicDatasetProfiler" in exp.meta
        assert "confidence" in exp.meta["BasicDatasetProfiler"]

    expected_expectations = {
        "expect_table_row_count_to_be_between",
        "expect_table_columns_to_match_ordered_list",
        "expect_column_values_to_be_in_set",
        "expect_column_unique_value_count_to_be_between",
        "expect_column_proportion_of_unique_values_to_be_between",
        "expect_column_values_to_not_be_null",
        "expect_column_values_to_be_in_type_list",
        "expect_column_values_to_be_unique",
    }

    assert expected_expectations.issubset(added_expectations)

    # Note 20211209 - Currently the only method called by the Profiler that is instrumented for usage_statistics
    # is ExpectationSuite's add_expectation(). It will not send a usage_stats event when called from a Profiler.
    # this number can change in the future our instrumentation changes.
    assert mock_emit.call_count == 0
    assert mock_emit.call_args_list == []


@pytest.mark.unit
def test_BasicDatasetProfiler_null_column():
    """
    The profiler should determine that null columns are of null cardinality and of null type and
    not to generate expectations specific to types and cardinality categories.

    We verify this by running the basic profiler on a Pandas dataset with an empty column
    and asserting the number of successful results for the empty columns.
    """
    toy_dataset = PandasDataset({"x": [1, 2, 3], "y": [None, None, None]})
    assert (
        len(toy_dataset.get_expectation_suite(suppress_warnings=True).expectations) == 0
    )

    expectations_config, evr_config = BasicDatasetProfiler.profile(toy_dataset)

    # TODO: assert set - specific expectations
    assert (
        len(
            [
                result
                for result in evr_config["results"]
                if result.expectation_config["kwargs"].get("column") == "y"
                and result.success
            ]
        )
        == 4
    )

    assert len(
        [
            result
            for result in evr_config["results"]
            if result.expectation_config["kwargs"].get("column") == "y"
            and result.success
        ]
    ) < len(
        [
            result
            for result in evr_config["results"]
            if result.expectation_config["kwargs"].get("column") == "x"
            and result.success
        ]
    )


@pytest.mark.filesystem
def test_BasicDatasetProfiler_with_context(filesystem_csv_data_context):
    context = filesystem_csv_data_context

    context.add_expectation_suite("default")
    datasource = context.datasources["rad_datasource"]
    base_dir = datasource.config["batch_kwargs_generators"]["subdir_reader"][
        "base_directory"
    ]
    batch_kwargs = {
        "datasource": "rad_datasource",
        "path": os.path.join(base_dir, "f1.csv"),  # noqa: PTH118
    }
    batch = context._get_batch_v2(batch_kwargs, "default")  # noqa: SLF001
    expectation_suite, validation_results = BasicDatasetProfiler.profile(batch)

    assert expectation_suite.expectation_suite_name == "default"
    assert "BasicDatasetProfiler" in expectation_suite.meta
    assert set(expectation_suite.meta["BasicDatasetProfiler"].keys()) == {
        "created_by",
        "created_at",
        "batch_kwargs",
    }
    assert (
        expectation_suite.meta["BasicDatasetProfiler"]["batch_kwargs"] == batch_kwargs
    )
    for exp in expectation_suite.expectation_configurations:
        assert "BasicDatasetProfiler" in exp.meta
        assert "confidence" in exp.meta["BasicDatasetProfiler"]

    assert set(validation_results.meta.keys()) == {
        "batch_kwargs",
        "batch_markers",
        "batch_parameters",
        "expectation_suite_meta",
        "expectation_suite_name",
        "great_expectations_version",
        "run_id",
        "validation_time",
    }
