import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.dataset.pandas_dataset import PandasDataset
from great_expectations.datasource import PandasDatasource
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


@pytest.mark.filesystem
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

    assert profiling_result["success"] is True
    assert len(profiling_result["results"]) == 1
    assert (
        profiling_result["results"][0][0].expectation_suite_name
        == "rad_datasource.subdir_reader.f1.BasicDatasetProfiler"
    )


@pytest.mark.filesystem
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


@pytest.mark.filesystem
def test_context_profiler_with_non_existing_generator(filesystem_csv_data_context):
    """
        If a non-existing generator name is passed to the profiling method
    in the generator_name argument, the profiling method must raise an exception.
    """
    context = filesystem_csv_data_context

    assert isinstance(context.datasources["rad_datasource"], PandasDatasource)
    assert context.list_expectation_suites() == []
    with pytest.raises(gx_exceptions.ProfilerError):
        profiling_result = context.profile_datasource(  # noqa: F841
            "rad_datasource",
            data_assets=["this_asset_doesnot_exist"],
            profiler=BasicDatasetProfiler,
            batch_kwargs_generator_name="this_gen_does_not_exist",
        )


@pytest.mark.filesystem
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
