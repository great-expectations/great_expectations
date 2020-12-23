import pytest

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.data_asset import DataAsset
from great_expectations.dataset import MetaPandasDataset, PandasDataset


class ExpectationOnlyDataAsset(DataAsset):
    @DataAsset.expectation([])
    def no_op_expectation(
        self, result_format=None, include_config=True, catch_exceptions=None, meta=None
    ):
        return {"success": True}

    @DataAsset.expectation(["value"])
    def no_op_value_expectation(
        self,
        value=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        return {"success": True}

    @DataAsset.expectation([])
    def exception_expectation(
        self, result_format=None, include_config=True, catch_exceptions=None, meta=None
    ):
        raise ValueError("Gotcha!")


def test_expectation_decorator_build_config():
    eds = ExpectationOnlyDataAsset()
    eds.no_op_expectation()
    eds.no_op_value_expectation("a")

    config = eds.get_expectation_suite()
    assert (
        ExpectationConfiguration(expectation_type="no_op_expectation", kwargs={})
        == config.expectations[0]
    )

    assert (
        ExpectationConfiguration(
            expectation_type="no_op_value_expectation",
            kwargs={"value": "a"},
        )
        == config.expectations[1]
    )


def test_expectation_decorator_include_config():
    eds = ExpectationOnlyDataAsset()
    out = eds.no_op_value_expectation("a", include_config=True)

    assert (
        ExpectationConfiguration(
            expectation_type="no_op_value_expectation",
            kwargs={"value": "a", "result_format": "BASIC"},
        )
        == out.expectation_config
    )


def test_expectation_decorator_meta():
    metadata = {"meta_key": "meta_value"}
    eds = ExpectationOnlyDataAsset()
    out = eds.no_op_value_expectation("a", meta=metadata)
    config = eds.get_expectation_suite()

    assert (
        ExpectationValidationResult(
            success=True, meta=metadata, expectation_config=config.expectations[0]
        )
        == out
    )

    assert (
        ExpectationConfiguration(
            expectation_type="no_op_value_expectation",
            kwargs={"value": "a"},
            meta=metadata,
        )
        == config.expectations[0]
    )


def test_expectation_decorator_catch_exceptions():
    eds = ExpectationOnlyDataAsset()

    # Confirm that we would raise an error without catching exceptions
    with pytest.raises(ValueError):
        eds.exception_expectation(catch_exceptions=False)

    # Catch exceptions and validate results
    out = eds.exception_expectation(catch_exceptions=True)
    assert out.exception_info["raised_exception"] is True

    # Check only the first and last line of the traceback, since formatting can be platform dependent.
    assert (
        "Traceback (most recent call last):"
        == out.exception_info["exception_traceback"].split("\n")[0]
    )
    assert (
        "ValueError: Gotcha!"
        == out.exception_info["exception_traceback"].split("\n")[-2]
    )


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


def test_column_map_expectation_decorator():

    # Create a new CustomPandasDataset to
    # (1) demonstrate that custom subclassing works, and
    # (2) Test expectation business logic without dependencies on any other functions.
    class CustomPandasDataset(PandasDataset):
        @MetaPandasDataset.column_map_expectation
        def expect_column_values_to_be_odd(self, column):
            return column.map(lambda x: x % 2)

        @MetaPandasDataset.column_map_expectation
        def expectation_that_crashes_on_sixes(self, column):
            return column.map(lambda x: (x - 6) / 0 != "duck")

    df = CustomPandasDataset(
        {
            "all_odd": [1, 3, 5, 5, 5, 7, 9, 9, 9, 11],
            "mostly_odd": [1, 3, 5, 7, 9, 2, 4, 1, 3, 5],
            "all_even": [2, 4, 4, 6, 6, 6, 8, 8, 8, 8],
            "odd_missing": [1, 3, 5, None, None, None, None, 1, 3, None],
            "mixed_missing": [1, 3, 5, None, None, 2, 4, 1, 3, None],
            "all_missing": [None, None, None, None, None, None, None, None, None, None],
        }
    )
    df.set_default_expectation_argument("result_format", "COMPLETE")
    df.set_default_expectation_argument("include_config", False)

    assert df.expect_column_values_to_be_odd("all_odd") == ExpectationValidationResult(
        result={
            "element_count": 10,
            "missing_count": 0,
            "missing_percent": 0.0,
            "partial_unexpected_counts": [],
            "partial_unexpected_index_list": [],
            "partial_unexpected_list": [],
            "unexpected_count": 0,
            "unexpected_index_list": [],
            "unexpected_list": [],
            "unexpected_percent": 0.0,
            "unexpected_percent_nonmissing": 0.0,
        },
        success=True,
    )

    assert df.expect_column_values_to_be_odd(
        "all_missing"
    ) == ExpectationValidationResult(
        result={
            "element_count": 10,
            "missing_count": 10,
            "missing_percent": 100.0,
            "partial_unexpected_counts": [],
            "partial_unexpected_index_list": [],
            "partial_unexpected_list": [],
            "unexpected_count": 0,
            "unexpected_index_list": [],
            "unexpected_list": [],
            "unexpected_percent": 0.0,
            "unexpected_percent_nonmissing": None,
        },
        success=True,
    )

    assert df.expect_column_values_to_be_odd(
        "odd_missing"
    ) == ExpectationValidationResult(
        result={
            "element_count": 10,
            "missing_count": 5,
            "missing_percent": 50.0,
            "partial_unexpected_counts": [],
            "partial_unexpected_index_list": [],
            "partial_unexpected_list": [],
            "unexpected_count": 0,
            "unexpected_index_list": [],
            "unexpected_list": [],
            "unexpected_percent": 0.0,
            "unexpected_percent_nonmissing": 0.0,
        },
        success=True,
    )

    assert df.expect_column_values_to_be_odd(
        "mixed_missing"
    ) == ExpectationValidationResult(
        result={
            "element_count": 10,
            "missing_count": 3,
            "missing_percent": 30.0,
            "partial_unexpected_counts": [
                {"value": 2.0, "count": 1},
                {"value": 4.0, "count": 1},
            ],
            "partial_unexpected_index_list": [5, 6],
            "partial_unexpected_list": [2.0, 4.0],
            "unexpected_count": 2,
            "unexpected_index_list": [5, 6],
            "unexpected_list": [2, 4],
            "unexpected_percent": 20.0,
            "unexpected_percent_nonmissing": (2 / 7 * 100),
        },
        success=False,
    )

    assert df.expect_column_values_to_be_odd(
        "mostly_odd"
    ) == ExpectationValidationResult(
        result={
            "element_count": 10,
            "missing_count": 0,
            "missing_percent": 0,
            "partial_unexpected_counts": [
                {"value": 2.0, "count": 1},
                {"value": 4.0, "count": 1},
            ],
            "partial_unexpected_index_list": [5, 6],
            "partial_unexpected_list": [2.0, 4.0],
            "unexpected_count": 2,
            "unexpected_index_list": [5, 6],
            "unexpected_list": [2, 4],
            "unexpected_percent": 20.0,
            "unexpected_percent_nonmissing": 20.0,
        },
        success=False,
    )

    assert df.expect_column_values_to_be_odd(
        "mostly_odd", mostly=0.6
    ) == ExpectationValidationResult(
        result={
            "element_count": 10,
            "missing_count": 0,
            "missing_percent": 0,
            "partial_unexpected_counts": [
                {"value": 2.0, "count": 1},
                {"value": 4.0, "count": 1},
            ],
            "partial_unexpected_index_list": [5, 6],
            "partial_unexpected_list": [2.0, 4.0],
            "unexpected_count": 2,
            "unexpected_index_list": [5, 6],
            "unexpected_list": [2, 4],
            "unexpected_percent": 20.0,
            "unexpected_percent_nonmissing": 20.0,
        },
        success=True,
    )

    assert df.expect_column_values_to_be_odd(
        "mostly_odd", result_format="BOOLEAN_ONLY"
    ) == ExpectationValidationResult(success=False)

    df.default_expectation_args["result_format"] = "BOOLEAN_ONLY"

    assert df.expect_column_values_to_be_odd(
        "mostly_odd"
    ) == ExpectationValidationResult(success=False)

    df.default_expectation_args["result_format"] = "BASIC"

    assert df.expect_column_values_to_be_odd(
        "mostly_odd", include_config=True
    ) == ExpectationValidationResult(
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_odd",
            kwargs={"column": "mostly_odd", "result_format": "BASIC"},
        ),
        result={
            "element_count": 10,
            "missing_count": 0,
            "missing_percent": 0,
            "partial_unexpected_list": [2, 4],
            "unexpected_count": 2,
            "unexpected_percent": 20.0,
            "unexpected_percent_nonmissing": 20.0,
        },
        success=False,
    )


def test_column_aggregate_expectation_decorator():
    # Create a new CustomPandasDataset to
    # (1) demonstrate that custom subclassing works, and
    # (2) Test expectation business logic without dependencies on any other functions.
    class CustomPandasDataset(PandasDataset):
        @PandasDataset.column_aggregate_expectation
        def expect_column_median_to_be_odd(self, column):
            median = self.get_column_median(column)
            return {"success": median % 2, "result": {"observed_value": median}}

    df = CustomPandasDataset(
        {
            "all_odd": [1, 3, 5, 7, 9],
            "all_even": [2, 4, 6, 8, 10],
            "odd_missing": [1, 3, 5, None, None],
            "mixed_missing": [1, 2, None, None, 6],
            "mixed_missing_2": [1, 3, None, None, 6],
            "all_missing": [
                None,
                None,
                None,
                None,
                None,
            ],
        }
    )
    df.set_default_expectation_argument("result_format", "COMPLETE")
    df.set_default_expectation_argument("include_config", False)

    assert df.expect_column_median_to_be_odd("all_odd") == ExpectationValidationResult(
        result={
            "observed_value": 5,
            "element_count": 5,
            "missing_count": 0,
            "missing_percent": 0,
        },
        success=True,
    )

    assert df.expect_column_median_to_be_odd("all_even") == ExpectationValidationResult(
        result={
            "observed_value": 6,
            "element_count": 5,
            "missing_count": 0,
            "missing_percent": 0,
        },
        success=False,
    )

    assert df.expect_column_median_to_be_odd(
        "all_even", result_format="SUMMARY"
    ) == ExpectationValidationResult(
        result={
            "observed_value": 6,
            "element_count": 5,
            "missing_count": 0,
            "missing_percent": 0,
        },
        success=False,
    )

    assert df.expect_column_median_to_be_odd(
        "all_even", result_format="BOOLEAN_ONLY"
    ) == ExpectationValidationResult(success=False)

    df.default_expectation_args["result_format"] = "BOOLEAN_ONLY"
    assert df.expect_column_median_to_be_odd("all_even") == ExpectationValidationResult(
        success=False
    )

    assert df.expect_column_median_to_be_odd(
        "all_even", result_format="BASIC"
    ) == ExpectationValidationResult(
        result={
            "observed_value": 6,
            "element_count": 5,
            "missing_count": 0,
            "missing_percent": 0,
        },
        success=False,
    )


def test_column_pair_map_expectation_decorator():
    # Create a new CustomPandasDataset to
    # (1) Demonstrate that custom subclassing works, and
    # (2) Test expectation business logic without dependencies on any other functions.
    class CustomPandasDataset(PandasDataset):
        @PandasDataset.column_pair_map_expectation
        def expect_column_pair_values_to_be_different(
            self,
            column_A,
            column_B,
            keep_missing="either",
            output_format=None,
            include_config=True,
            catch_exceptions=None,
        ):
            return column_A != column_B

    df = CustomPandasDataset(
        {
            "all_odd": [1, 3, 5, 7, 9],
            "all_even": [2, 4, 6, 8, 10],
            "odd_missing": [1, 3, 5, None, None],
            "mixed_missing": [1, 2, None, None, 6],
            "mixed_missing_2": [1, 3, None, None, 6],
            "all_missing": [
                None,
                None,
                None,
                None,
                None,
            ],
        }
    )
    df.set_default_expectation_argument("result_format", "COMPLETE")
    df.set_default_expectation_argument("include_config", False)

    assert df.expect_column_pair_values_to_be_different(
        "all_odd", "all_even"
    ) == ExpectationValidationResult(
        success=True,
        result={
            "element_count": 5,
            "missing_count": 0,
            "unexpected_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent": 0.0,
            "unexpected_percent_nonmissing": 0.0,
            "unexpected_list": [],
            "unexpected_index_list": [],
            "partial_unexpected_list": [],
            "partial_unexpected_index_list": [],
            "partial_unexpected_counts": [],
        },
    )

    assert df.expect_column_pair_values_to_be_different(
        "all_odd",
        "all_even",
        ignore_row_if="both_values_are_missing",
    ) == ExpectationValidationResult(
        success=True,
        result={
            "element_count": 5,
            "missing_count": 0,
            "unexpected_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent": 0.0,
            "unexpected_percent_nonmissing": 0.0,
            "unexpected_list": [],
            "unexpected_index_list": [],
            "partial_unexpected_list": [],
            "partial_unexpected_index_list": [],
            "partial_unexpected_counts": [],
        },
    )

    assert df.expect_column_pair_values_to_be_different(
        "all_odd", "odd_missing"
    ) == ExpectationValidationResult(
        success=False,
        result={
            "element_count": 5,
            "missing_count": 0,
            "unexpected_count": 3,
            "missing_percent": 0.0,
            "unexpected_percent": 60.0,
            "unexpected_percent_nonmissing": 60.0,
            "unexpected_list": [(1, 1.0), (3, 3.0), (5, 5.0)],
            "unexpected_index_list": [0, 1, 2],
            "partial_unexpected_list": [(1, 1.0), (3, 3.0), (5, 5.0)],
            "partial_unexpected_index_list": [0, 1, 2],
            "partial_unexpected_counts": [
                {"count": 1, "value": (1, 1.0)},
                {"count": 1, "value": (3, 3.0)},
                {"count": 1, "value": (5, 5.0)},
            ],
        },
    )

    assert df.expect_column_pair_values_to_be_different(
        "all_odd", "odd_missing", ignore_row_if="both_values_are_missing"
    ) == ExpectationValidationResult(
        success=False,
        result={
            "element_count": 5,
            "missing_count": 0,
            "unexpected_count": 3,
            "missing_percent": 0.0,
            "unexpected_percent": 60.0,
            "unexpected_percent_nonmissing": 60.0,
            "unexpected_list": [(1, 1.0), (3, 3.0), (5, 5.0)],
            "unexpected_index_list": [0, 1, 2],
            "partial_unexpected_list": [(1, 1.0), (3, 3.0), (5, 5.0)],
            "partial_unexpected_index_list": [0, 1, 2],
            "partial_unexpected_counts": [
                {"count": 1, "value": (1, 1.0)},
                {"count": 1, "value": (3, 3.0)},
                {"count": 1, "value": (5, 5.0)},
            ],
        },
    )

    assert df.expect_column_pair_values_to_be_different(
        "all_odd", "odd_missing", ignore_row_if="either_value_is_missing"
    ) == ExpectationValidationResult(
        success=False,
        result={
            "element_count": 5,
            "missing_count": 2,
            "unexpected_count": 3,
            "missing_percent": 40.0,
            "unexpected_percent": 60.0,
            "unexpected_percent_nonmissing": 100.0,
            "unexpected_list": [(1, 1.0), (3, 3.0), (5, 5.0)],
            "unexpected_index_list": [0, 1, 2],
            "partial_unexpected_list": [(1, 1.0), (3, 3.0), (5, 5.0)],
            "partial_unexpected_index_list": [0, 1, 2],
            "partial_unexpected_counts": [
                {"count": 1, "value": (1, 1.0)},
                {"count": 1, "value": (3, 3.0)},
                {"count": 1, "value": (5, 5.0)},
            ],
        },
    )

    with pytest.raises(ValueError):
        df.expect_column_pair_values_to_be_different(
            "all_odd", "odd_missing", ignore_row_if="blahblahblah"
        )

    # Test SUMMARY, BASIC, and BOOLEAN_ONLY output_formats
    assert df.expect_column_pair_values_to_be_different(
        "all_odd", "all_even", result_format="SUMMARY"
    ) == ExpectationValidationResult(
        success=True,
        result={
            "element_count": 5,
            "missing_count": 0,
            "unexpected_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent": 0.0,
            "unexpected_percent_nonmissing": 0.0,
            "partial_unexpected_list": [],
            "partial_unexpected_index_list": [],
            "partial_unexpected_counts": [],
        },
    )

    assert df.expect_column_pair_values_to_be_different(
        "all_odd", "all_even", result_format="BASIC"
    ) == ExpectationValidationResult(
        success=True,
        result={
            "element_count": 5,
            "missing_count": 0,
            "unexpected_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent": 0.0,
            "unexpected_percent_nonmissing": 0.0,
            "partial_unexpected_list": [],
        },
    )

    assert df.expect_column_pair_values_to_be_different(
        "all_odd", "all_even", result_format="BOOLEAN_ONLY"
    ) == ExpectationValidationResult(success=True)
