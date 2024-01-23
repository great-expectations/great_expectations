import pytest

from great_expectations.dataset.pandas_dataset import PandasDataset
from great_expectations.profile.base import DatasetProfiler, Profiler
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

    expectations_config, _evr_config = ColumnsExistProfiler.profile(toy_dataset)

    assert len(expectations_config.expectations) == 1
    assert (
        expectations_config.expectation_configurations[0].expectation_type
        == "expect_column_to_exist"
    )
    assert expectations_config.expectation_configurations[0].kwargs["column"] == "x"
