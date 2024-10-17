import timeit

import pandas as pd
import pytest

from great_expectations.compatibility import aws
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.expectations import (
    ExpectColumnValuesToBeInSet,
)
from great_expectations.self_check.util import build_sa_validator_with_data
from great_expectations.util import is_library_loadable


@pytest.mark.skipif(
    not (aws.sqlalchemy_athena and is_library_loadable(library_name="pyathena")),
    reason="pyathena is not installed",
)
@pytest.mark.athena
@pytest.mark.external_sqldialect
def test_expect_column_values_to_be_of_type_string_dialect_pyathena(sa):
    df = pd.DataFrame({"col": ["test_val1", "test_val2"]})
    validator = build_sa_validator_with_data(
        df=df,
        sa_engine_name="sqlite",
        table_name="expect_column_values_to_be_of_type_string_dialect_pyathena_1",
    )

    # Monkey-patch dialect for testing purposes.
    validator.execution_engine.dialect_module = aws.sqlalchemy_athena

    result = validator.expect_column_values_to_be_of_type("col", type_="string")

    assert result == ExpectationValidationResult(
        success=True,
        expectation_config={
            "expectation_type": "expect_column_values_to_be_of_type",
            "kwargs": {
                "column": "col",
                "type_": "string",
            },
            "meta": {},
        },
        result={
            "element_count": 2,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "partial_unexpected_list": [],
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent_total": 0.0,
            "unexpected_percent_nonmissing": 0.0,
        },
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        meta={},
    )


@pytest.mark.sqlite
@pytest.mark.external_sqldialect
def test_expect_column_values_to_be_of_type_string_dialect_sqlite(sa):
    df = pd.DataFrame({"col": ["test_val1", "test_val2"]})
    validator = build_sa_validator_with_data(
        df=df,
        sa_engine_name="sqlite",
        table_name="expect_column_values_to_be_of_type_string_dialect_sqlite_1",
    )

    result = validator.expect_column_values_to_be_of_type("col", type_="TEXT")

    assert result == ExpectationValidationResult(
        success=True,
        expectation_config={
            "expectation_type": "expect_column_values_to_be_of_type",
            "kwargs": {
                "column": "col",
                "type_": "TEXT",
            },
            "meta": {},
        },
        result={
            "element_count": 2,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "partial_unexpected_list": [],
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent_total": 0.0,
            "unexpected_percent_nonmissing": 0.0,
        },
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        meta={},
    )


@pytest.mark.unit
def test_expect_column_values_to_be_in_set_render_performance():
    """
    This test prevents a historical bug in which rendering took ~10 seconds to render 400 items.
    """

    large_number = 400

    x = ExpectColumnValuesToBeInSet(
        column="foo_column_name", value_set=["foo" for _ in range(large_number)]
    )

    duration_s = timeit.timeit(x.render, number=1)
    assert duration_s < 2, f"Rendering took {duration_s} seconds"
