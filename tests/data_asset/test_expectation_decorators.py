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


@pytest.mark.unit
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


@pytest.mark.unit
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


@pytest.mark.unit
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


@pytest.mark.unit
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
