import pytest

import json

from great_expectations.profile.base import DataSetProfiler
from great_expectations.profile.pseudo_pandas_profiling import PseudoPandasProfiler
from great_expectations.profile.columns_exist import ColumnsExistProfiler
from great_expectations.dataset.pandas_dataset import PandasDataset


def test_DataSetProfiler_methods():
    toy_dataset = PandasDataset({"x": [1, 2, 3]})

    assert DataSetProfiler.validate_dataset(1) == False
    assert DataSetProfiler.validate_dataset(toy_dataset)

    with pytest.raises(NotImplementedError) as e_info:
        DataSetProfiler.profile(toy_dataset)


def test_ColumnsExistProfiler():
    toy_dataset = PandasDataset({"x": [1, 2, 3]})
    results = ColumnsExistProfiler.profile(toy_dataset)

    print(json.dumps(results, indent=2))

    assert results == {
        "data_asset_name": None,
        "data_asset_type": "Dataset",
        "meta": {
            "great_expectations.__version__": "0.7.0-beta"
        },
        "expectations": [
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {
                    "column": "x"
                }
            }
        ]
    }


def test_PseudoPandasProfiler():
    toy_dataset = PandasDataset({"x": [1, 2, 3]})
    assert len(toy_dataset.get_expectations(
        suppress_warnings=True)["expectations"]) == 0

    results = PseudoPandasProfiler.profile(toy_dataset)

    print(json.dumps(results, indent=2))

    assert len(toy_dataset.get_expectations(
        suppress_warnings=True)["expectations"]) > 0
