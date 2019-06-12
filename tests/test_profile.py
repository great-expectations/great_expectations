import pytest

from great_expectations.profile.base import DataSetProfiler
from great_expectations.dataset.pandas_dataset import PandasDataset


def test_DataSetProfiler_methods():
    toy_dataset = PandasDataset({"x": [1, 2, 3]})

    assert DataSetProfiler.validate_dataset(1) == False
    assert DataSetProfiler.validate_dataset(toy_dataset)

    with pytest.raises(NotImplementedError) as e_info:
        DataSetProfiler.profile(toy_dataset)
