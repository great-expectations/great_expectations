import pytest

from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.data_asset import DataAsset
from great_expectations.dataset import MetaPandasDataset, PandasDataset


@pytest.mark.unit
def test_pandas_column_map_decorator_partial_exception_counts():
    df = PandasDataset({"a": [0, 1, 2, 3, 4]})
    out = df.expect_column_values_to_be_between(
        "a",
        3,
        4,
        result_format={"result_format": "COMPLETE", "partial_unexpected_count": 1},
    )

    assert 1 == len(out.result["partial_unexpected_counts"])
    assert 3 == len(out.result["unexpected_list"])
