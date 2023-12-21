import os

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
    batch = context._get_batch_v2(batch_kwargs, "default")
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
