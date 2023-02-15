import pandas as pd
import pytest

from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.dataset.pandas_dataset import PandasDataset


@pytest.fixture
def test_df():
    return pd.DataFrame({"col_1": [1, 2], "col_2": ["one", "two"]})


@pytest.mark.unit
def test_expectation_suite_extract_false_no_results(test_df):
    test_ds = PandasDataset(data=test_df)

    test_ds.expect_column_values_to_be_of_type("col_1", "int")
    test_ds.expect_column_values_to_be_of_type("col_2", "object")

    result = test_ds.validate()

    failed_results = result.get_failed_validation_results()

    assert isinstance(failed_results, ExpectationSuiteValidationResult)
    assert failed_results.statistics["evaluated_expectations"] == 0


@pytest.mark.unit
def test_expectation_suite_extract_false_many_results(test_df):
    test_ds = PandasDataset(data=test_df)

    test_ds.expect_column_values_to_be_of_type("col_1", "boolean")
    test_ds.expect_column_values_to_be_of_type("col_2", "object")
    test_ds.expect_column_values_to_be_null("col_1")

    result = test_ds.validate()

    failed_results = result.get_failed_validation_results()

    assert isinstance(failed_results, ExpectationSuiteValidationResult)
    assert failed_results.statistics["evaluated_expectations"] == 2
    assert result.statistics["evaluated_expectations"] == 3
