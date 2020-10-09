import os

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.dataset.pandas_dataset import PandasDataset
from great_expectations.datasource import PandasDatasource
from great_expectations.profile.base import DatasetProfiler, Profiler
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.profile.columns_exist import ColumnsExistProfiler


def test_base_class_not_instantiable_due_to_abstract_methods():
    with pytest.raises(TypeError):
        Profiler()


def test_DataSetProfiler_methods():
    toy_dataset = PandasDataset({"x": [1, 2, 3]})

    assert DatasetProfiler.validate(1) == False
    assert DatasetProfiler.validate(toy_dataset)

    with pytest.raises(NotImplementedError):
        DatasetProfiler.profile(toy_dataset)


def test_ColumnsExistProfiler():
    toy_dataset = PandasDataset({"x": [1, 2, 3]})

    expectations_config, evr_config = ColumnsExistProfiler.profile(toy_dataset)

    assert len(expectations_config.expectations) == 1
    assert (
        expectations_config.expectations[0].expectation_type == "expect_column_to_exist"
    )
    assert expectations_config.expectations[0].kwargs["column"] == "x"


def test_BasicDatasetProfiler():
    toy_dataset = PandasDataset({"x": [1, 2, 3]},)
    assert (
        len(toy_dataset.get_expectation_suite(suppress_warnings=True).expectations) == 0
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
    for exp in expectations_config.expectations:
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


def test_BasicDatasetProfiler_partially_null_column(dataset):
    """
    Unit test to check the expectations that BasicDatasetProfiler creates for a partially null column.
    The test is executed against all the backends (Pandas, Spark, etc.), because it uses
    the fixture.

    "nulls" is the partially null column in the fixture dataset
    """
    expectations_config, evr_config = BasicDatasetProfiler.profile(dataset)

    assert {
        "expect_column_to_exist",
        "expect_column_values_to_be_in_type_list",
        "expect_column_unique_value_count_to_be_between",
        "expect_column_proportion_of_unique_values_to_be_between",
        "expect_column_values_to_not_be_null",
        "expect_column_values_to_be_in_set",
        "expect_column_values_to_be_unique",
    } == {
        expectation.expectation_type
        for expectation in expectations_config.expectations
        if expectation.kwargs.get("column") == "nulls"
    }


def test_BasicDatasetProfiler_non_numeric_low_cardinality(non_numeric_low_card_dataset):
    """
    Unit test to check the expectations that BasicDatasetProfiler creates for a low cardinality
    non numeric column.
    The test is executed against all the backends (Pandas, Spark, etc.), because it uses
    the fixture.
    """
    expectations_config, evr_config = BasicDatasetProfiler.profile(
        non_numeric_low_card_dataset
    )

    assert {
        "expect_column_to_exist",
        "expect_column_values_to_be_in_type_list",
        "expect_column_unique_value_count_to_be_between",
        "expect_column_distinct_values_to_be_in_set",
        "expect_column_proportion_of_unique_values_to_be_between",
        "expect_column_values_to_not_be_null",
        "expect_column_values_to_be_in_set",
        "expect_column_values_to_not_match_regex",
    } == {
        expectation.expectation_type
        for expectation in expectations_config.expectations
        if expectation.kwargs.get("column") == "lowcardnonnum"
    }


def test_BasicDatasetProfiler_non_numeric_high_cardinality(
    non_numeric_high_card_dataset,
):
    """
    Unit test to check the expectations that BasicDatasetProfiler creates for a high cardinality
    non numeric column.
    The test is executed against all the backends (Pandas, Spark, etc.), because it uses
    the fixture.
    """
    expectations_config, evr_config = BasicDatasetProfiler.profile(
        non_numeric_high_card_dataset
    )

    assert {
        "expect_column_to_exist",
        "expect_column_values_to_be_in_type_list",
        "expect_column_unique_value_count_to_be_between",
        "expect_column_proportion_of_unique_values_to_be_between",
        "expect_column_values_to_not_be_null",
        "expect_column_values_to_be_in_set",
        "expect_column_values_to_not_match_regex",
    } == {
        expectation.expectation_type
        for expectation in expectations_config.expectations
        if expectation.kwargs.get("column") == "highcardnonnum"
    }


def test_BasicDatasetProfiler_numeric_high_cardinality(numeric_high_card_dataset):
    """
    Unit test to check the expectations that BasicDatasetProfiler creates for a high cardinality
    numeric column.
    The test is executed against all the backends (Pandas, Spark, etc.), because it uses
    the fixture.
    """
    expectations_config, evr_config = BasicDatasetProfiler.profile(
        numeric_high_card_dataset
    )

    assert {
        "expect_column_to_exist",
        "expect_table_row_count_to_be_between",
        "expect_table_columns_to_match_ordered_list",
        "expect_column_values_to_be_in_type_list",
        "expect_column_unique_value_count_to_be_between",
        "expect_column_proportion_of_unique_values_to_be_between",
        "expect_column_values_to_not_be_null",
        "expect_column_values_to_be_in_set",
        "expect_column_values_to_be_unique",
    } == {
        expectation.expectation_type for expectation in expectations_config.expectations
    }


def test_BasicDatasetProfiler_with_context(filesystem_csv_data_context):
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
    for exp in expectation_suite.expectations:
        assert "BasicDatasetProfiler" in exp.meta
        assert "confidence" in exp.meta["BasicDatasetProfiler"]

    assert set(validation_results.meta.keys()) == {
        "batch_kwargs",
        "batch_markers",
        "batch_parameters",
        "expectation_suite_name",
        "great_expectations_version",
        "run_id",
        "validation_time",
    }


def test_context_profiler(filesystem_csv_data_context):
    """
    This just validates that it's possible to profile using the datasource hook,
    and have validation results available in the DataContext
    """
    context = filesystem_csv_data_context

    assert isinstance(context.datasources["rad_datasource"], PandasDatasource)
    assert context.list_expectation_suites() == []
    context.profile_datasource("rad_datasource", profiler=BasicDatasetProfiler)

    assert len(context.list_expectation_suites()) == 1

    expected_suite_name = "rad_datasource.subdir_reader.f1.BasicDatasetProfiler"
    profiled_expectations = context.get_expectation_suite(expected_suite_name)

    for exp in profiled_expectations.expectations:
        assert "BasicDatasetProfiler" in exp.meta
        assert "confidence" in exp.meta["BasicDatasetProfiler"]

    assert profiled_expectations.expectation_suite_name == expected_suite_name
    assert "batch_kwargs" in profiled_expectations.meta["BasicDatasetProfiler"]
    assert len(profiled_expectations.expectations) == 8


def test_context_profiler_with_data_asset_name(filesystem_csv_data_context):
    """
    If a valid data asset name is passed to the profiling method
    in the data_assets argument, the profiling method profiles only this data asset
    """
    context = filesystem_csv_data_context

    assert isinstance(context.datasources["rad_datasource"], PandasDatasource)
    assert context.list_expectation_suites() == []
    profiling_result = context.profile_datasource(
        "rad_datasource", data_assets=["f1"], profiler=BasicDatasetProfiler
    )

    assert profiling_result["success"] == True
    assert len(profiling_result["results"]) == 1
    assert (
        profiling_result["results"][0][0].expectation_suite_name
        == "rad_datasource.subdir_reader.f1.BasicDatasetProfiler"
    )


def test_context_profiler_with_nonexisting_data_asset_name(filesystem_csv_data_context):
    """
    If a non-existing data asset name is passed to the profiling method
    in the data_assets argument, the profiling method must return an error
    code in the result and the names of the unrecognized assets
    """
    context = filesystem_csv_data_context

    assert isinstance(context.datasources["rad_datasource"], PandasDatasource)
    assert context.list_expectation_suites() == []
    profiling_result = context.profile_datasource(
        "rad_datasource",
        data_assets=["this_asset_doesnot_exist"],
        profiler=BasicDatasetProfiler,
    )

    assert profiling_result == {
        "success": False,
        "error": {
            "code": 3,
            "not_found_data_assets": ["this_asset_doesnot_exist"],
            "data_assets": [("f1", "file")],
        },
    }


def test_context_profiler_with_non_existing_generator(filesystem_csv_data_context):
    """
    If a non-existing generator name is passed to the profiling method
in the generator_name argument, the profiling method must raise an exception.
    """
    context = filesystem_csv_data_context

    assert isinstance(context.datasources["rad_datasource"], PandasDatasource)
    assert context.list_expectation_suites() == []
    with pytest.raises(ge_exceptions.ProfilerError):
        profiling_result = context.profile_datasource(
            "rad_datasource",
            data_assets=["this_asset_doesnot_exist"],
            profiler=BasicDatasetProfiler,
            batch_kwargs_generator_name="this_gen_does_not_exist",
        )


def test_context_profiler_without_generator_name_arg_on_datasource_with_multiple_generators(
    filesystem_csv_data_context, filesystem_csv_2
):
    """
    If a no generator_name is passed to the profiling method and the datasource has more than one
    generators configured, the profiling method must return an error code in the result
    """
    context = filesystem_csv_data_context
    context.add_batch_kwargs_generator(
        "rad_datasource",
        "second_generator",
        "SubdirReaderBatchKwargsGenerator",
        **{"base_directory": str(filesystem_csv_2),}
    )

    assert isinstance(context.datasources["rad_datasource"], PandasDatasource)
    profiling_result = context.profile_datasource(
        "rad_datasource",
        data_assets=["this_asset_doesnot_exist"],
        profiler=BasicDatasetProfiler,
    )

    assert profiling_result == {"success": False, "error": {"code": 5}}


def test_context_profiler_without_generator_name_arg_on_datasource_with_no_generators(
    filesystem_csv_data_context,
):
    """
    If a no generator_name is passed to the profiling method and the datasource has no
    generators configured, the profiling method must return an error code in the result
    """
    context = filesystem_csv_data_context
    context.add_datasource(
        "datasource_without_generators",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
    )
    assert isinstance(
        context.datasources["datasource_without_generators"], PandasDatasource
    )
    profiling_result = context.profile_datasource(
        "datasource_without_generators", profiler=BasicDatasetProfiler
    )

    assert profiling_result == {"success": False, "error": {"code": 4}}
