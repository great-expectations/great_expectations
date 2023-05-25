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
    # This is the id string that will be used to reference your Metric.
    metric_name = "predicted_cardinality_category"

    # This method implements the core logic for the PandasExecutionEngine
    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, depth=2, **kwargs):
        n_unique: int = column.nunique()
        n_nonmissing: int = column.notnull().sum()
        total_to_unique_ratio: float = n_nonmissing / n_unique

        print(kwargs)

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


        if depth == 1:
            return {
                "FINITE": total_to_unique_ratio < 100,
                "INFINITE": total_to_unique_ratio > 100,
            }
        
        elif depth == 2:
            return {
                "UNIQUE": total_to_unique_ratio > 100,
                "DUPLICATED": total_to_unique_ratio < 1.1,
                "MANY": total_to_unique_ratio < 10,
                "SEVERAL": total_to_unique_ratio < 20,
                "A_FEW": total_to_unique_ratio < 100,
            }

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
            # {
            #     "title": "basic_positive_test",
            #     "exact_match_out": False,
            #     "include_in_gallery": True,
            #     "in": {
            #         "column": "infinite_with_repeats",
            #         "cardinality_category": "INFINITE",
            #         "depth": 1
            #     },
            #     "out": {
            #         "success": True
            #     },
            # },
        ],
        "only_for": ["pandas"],
        # "test_backends": [
        #     {
        #         "backend": "pandas",
        #         "dialects": None,
        #     },
        #     {
        #         "backend": "sqlalchemy",
        #         "dialects": None,
        #     },
        #     {
        #         "backend": "spark",
        #         "dialects": None,
        #     },
        # ],
    }]

    # This is a tuple consisting of all Metrics necessary to evaluate the Expectation.
    metric_dependencies = ("predicted_cardinality_category",)

    # This a tuple of parameter names that can affect whether the Expectation evaluates to True or False.
    success_keys = (
        "cardinality_category",
        # "depth",
    )

    # This dictionary contains default values for any parameters that should have default values.
    default_kwarg_values = {}

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        print("metrics: ", metrics)
        print(configuration.kwargs.get("cardinality_category"))

        predicted_cardinality_category = metrics["predicted_cardinality_category"]
        success = predicted_cardinality_category == configuration.kwargs.get("cardinality_category")

        rval = {
            "success": success,
            "result": {
                "observed_value": predicted_cardinality_category
            }
        }
        print("rval: ", rval)

        return rval

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@abegong",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnPredictedCardinalityCategoryToBe().print_diagnostic_checklist()