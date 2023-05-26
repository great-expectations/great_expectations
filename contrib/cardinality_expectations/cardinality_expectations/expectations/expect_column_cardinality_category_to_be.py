from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnAggregateExpectation
from great_expectations.expectations.metrics import (
    ColumnAggregateMetricProvider,
    column_aggregate_partial,
    column_aggregate_value,
)

FINITE_CATEGORIES = ["A_FEW", "SEVERAL", "MANY"]
INFINITE_CATEGORIES = ["UNIQUE", "DUPLICATED"]

class ColumnPredictedCardinalityCategory(ColumnAggregateMetricProvider):
    metric_name = "column.predicted_cardinality_category"

    value_keys = (
        "depth",
    )

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, depth, **kwargs):
        n_unique: int = column.nunique()
        n_nonmissing: int = column.notnull().sum()
        total_to_unique_ratio: float = n_nonmissing / n_unique

        if depth == 1:
            if total_to_unique_ratio < 10:
                return "INFINITE"
            else:
                return "FINITE"
        
        elif depth == 2:
            if n_unique < 7:
                return "A_FEW"
            elif n_unique < 20:
                return "SEVERAL"
            elif n_unique == n_nonmissing:
                return "UNIQUE"
            
            if total_to_unique_ratio < 10:
                return "DUPLICATED"
            else:
                return "MANY"

    # This method defines the business logic for evaluating your Metric when using a SqlAlchemyExecutionEngine
    # @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError
    #
    # This method defines the business logic for evaluating your Metric when using a SparkDFExecutionEngine
    # @column_aggregate_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     raise NotImplementedError


class ExpectColumnPredictedCardinalityCategoryToBe(ColumnAggregateExpectation):
    """This expectation predicts the cardinality category of a column. based on the number of unique values in the column.
    """

    examples = [{
        "data": {
            "infinite_and_unique": [i for i in range(100)],
            "infinite_with_repeats": [i/2 for i in range(100)],
            "many": [i%20 for i in range(100)],
            "several": [i%12 for i in range(100)],
            "a_few": [i%5 for i in range(100)],
            "a_couple": [i%2 for i in range(100)],
            "one": [1 for i in range(100)],
            "zero": [None for i in range(100)],
        },
        "tests": [
            {
                "title": "basic_positive_test",
                "exact_match_out": False,
                "include_in_gallery": True,
                "in": {
                    "column": "infinite_and_unique",
                    "cardinality_category": "UNIQUE"
                },
                "out": {
                    "success": True
                },
            },
            {
                "title": "basic_negative_test",
                "exact_match_out": False,
                "include_in_gallery": True,
                "in": {
                    "column": "many",
                    "cardinality_category": "UNIQUE"
                },
                "out": {"success": False},
            },
            {
                "title": "basic_positive_test_with_depth_1",
                "exact_match_out": False,
                "include_in_gallery": True,
                "in": {
                    "column": "infinite_and_unique",
                    "cardinality_category": "INFINITE",
                    "depth": 1
                },
                "out": {
                    "success": True
                },
            },
        ],
        "only_for": ["pandas"],
    }]

    metric_dependencies = ("column.predicted_cardinality_category",)

    success_keys = (
        "column",
        "cardinality_category",
        "depth",
    )

    default_kwarg_values = {
        "depth": 2,
    }

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        print("metrics: ", metrics)
        print(configuration.kwargs.get("cardinality_category"))

        predicted_cardinality_category = metrics["column.predicted_cardinality_category"]
        success = predicted_cardinality_category == configuration.kwargs.get("cardinality_category")

        rval = {
            "success": success,
            "result": {
                "observed_value": predicted_cardinality_category
            }
        }
        print("rval: ", rval)

        return rval

    library_metadata = {
        "tags": [],
        "contributors": [
            "@abegong",
        ],
    }


if __name__ == "__main__":
    ExpectColumnPredictedCardinalityCategoryToBe().print_diagnostic_checklist()