import pandas as pd

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyBatchData,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.core.expect_column_values_to_be_in_type_list import (
    ExpectColumnValuesToBeInTypeList,
)
from great_expectations.validator.validator import Validator


def test_expect_column_values_to_be_in_type_list_dialect_pyathena(sa):
    from pyathena import sqlalchemy_athena

    engine = sa.create_engine("sqlite://")

    data = pd.DataFrame({"col": ["test_val1", "test_val2"]})

    data.to_sql(name="test_sql_data", con=engine, index=False)

    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_type_list",
        kwargs={
            "column": "col",
            "type_list": ["STRINGTYPE", "BOOLEAN"],
        },
    )
    expectation = ExpectColumnValuesToBeInTypeList(expectationConfiguration)
    engine = SqlAlchemyExecutionEngine(engine=engine)
    # Monkey-patch dialect for testing purposes.
    engine.dialect_module = sqlalchemy_athena
    engine.load_batch_data(
        "my_id",
        SqlAlchemyBatchData(execution_engine=engine, table_name="test_sql_data"),
    )
    result = expectation.validate(Validator(execution_engine=engine))

    assert result == ExpectationValidationResult(
        success=True,
        expectation_config={
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "col",
                "type_list": ["STRINGTYPE", "BOOLEAN"],
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
