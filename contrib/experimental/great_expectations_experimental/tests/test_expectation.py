import pandas as pd
import pytest

from great_expectations.execution_engine import PandasExecutionEngine

# Define a sample dataset with various datetime scenarios
sample_data = {
    "start_datetime": [
        "2023-01-01",
        "2023-02-01",
        "2023-03-01",
        "2023-04-01",
        "2023-05-01",
    ],
    "end_datetime": [
        "2023-01-15",
        "2023-03-02",
        "2023-05-01",
        "2023-06-01",
        "2023-07-01",
    ],
}
sample_df = pd.DataFrame(sample_data)

# Initialize the PandasExecutionEngine
execution_engine = PandasExecutionEngine()


# Create test cases
def test_valid_inputs():
    expectation_config = {
        "expectation_type": "expect_multicolumn_datetime_difference_to_be_less_than_two_months",
        "kwargs": {
            "start_datetime": "start_datetime",
            "end_datetime": "end_datetime",
            "column_list": ["start_datetime", "end_datetime"],
        },
    }
    expectation = MulticolumnDatetimeDifferenceToBeLessThanTwoMonths(
        configuration=expectation_config
    )
    result = expectation.validate(sample_df, execution_engine=execution_engine)
    assert result["success"] is True


def test_missing_columns():
    # Test with missing columns in the configuration
    expectation_config = {
        "expectation_type": "expect_multicolumn_datetime_difference_to_be_less_than_two_months",
        "kwargs": {
            "start_datetime": "missing_start_datetime",
            "end_datetime": "missing_end_datetime",
            "column_list": ["missing_start_datetime", "missing_end_datetime"],
        },
    }
    expectation = MulticolumnDatetimeDifferenceToBeLessThanTwoMonths(
        configuration=expectation_config
    )
    result = expectation.validate(sample_df, execution_engine=execution_engine)
    assert result["success"] is False


def test_empty_data():
    # Test with an empty dataset
    empty_df = pd.DataFrame()
    expectation_config = {
        "expectation_type": "expect_multicolumn_datetime_difference_to_be_less_than_two_months",
        "kwargs": {
            "start_datetime": "start_datetime",
            "end_datetime": "end_datetime",
            "column_list": ["start_datetime", "end_datetime"],
        },
    }
    expectation = MulticolumnDatetimeDifferenceToBeLessThanTwoMonths(
        configuration=expectation_config
    )
    result = expectation.validate(empty_df, execution_engine=execution_engine)
    assert result["success"] is True


def test_nan_values():
    # Test with NaN values in the dataset
    nan_data = {
        "start_datetime": [pd.NaT, "2023-02-01", "2023-03-01"],
        "end_datetime": ["2023-01-15", "2023-03-02", pd.NaT],
    }
    nan_df = pd.DataFrame(nan_data)
    expectation_config = {
        "expectation_type": "expect_multicolumn_datetime_difference_to_be_less_than_two_months",
        "kwargs": {
            "start_datetime": "start_datetime",
            "end_datetime": "end_datetime",
            "column_list": ["start_datetime", "end_datetime"],
        },
    }
    expectation = MulticolumnDatetimeDifferenceToBeLessThanTwoMonths(
        configuration=expectation_config
    )
    result = expectation.validate(nan_df, execution_engine=execution_engine)
    assert result["success"] is True


def test_datetime_difference_within_threshold():
    # Test with datetime differences within the threshold
    within_threshold_data = {
        "start_datetime": ["2023-01-01", "2023-02-01", "2023-03-01", "2023-04-01"],
        "end_datetime": ["2023-01-15", "2023-03-02", "2023-03-01", "2023-05-01"],
    }
    within_threshold_df = pd.DataFrame(within_threshold_data)
    expectation_config = {
        "expectation_type": "expect_multicolumn_datetime_difference_to_be_less_than_two_months",
        "kwargs": {
            "start_datetime": "start_datetime",
            "end_datetime": "end_datetime",
            "column_list": ["start_datetime", "end_datetime"],
        },
    }
    expectation = MulticolumnDatetimeDifferenceToBeLessThanTwoMonths(
        configuration=expectation_config
    )
    result = expectation.validate(
        within_threshold_df, execution_engine=execution_engine
    )
    assert result["success"] is True


def test_datetime_difference_above_threshold():
    # Test with datetime differences above the threshold
    above_threshold_data = {
        "start_datetime": ["2023-01-01", "2023-02-01", "2023-03-01", "2023-04-01"],
        "end_datetime": ["2023-01-15", "2023-03-02", "2023-05-01", "2023-07-01"],
    }
    above_threshold_df = pd.DataFrame(above_threshold_data)
    expectation_config = {
        "expectation_type": "expect_multicolumn_datetime_difference_to_be_less_than_two_months",
        "kwargs": {
            "start_datetime": "start_datetime",
            "end_datetime": "end_datetime",
            "column_list": ["start_datetime", "end_datetime"],
        },
    }
    expectation = MulticolumnDatetimeDifferenceToBeLessThanTwoMonths(
        configuration=expectation_config
    )
    result = expectation.validate(above_threshold_df, execution_engine=execution_engine)
    assert result["success"] is False


if __name__ == "__main__":
    pytest.main()
