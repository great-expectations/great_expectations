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
    """This expectation predicts the cardinality category of a column.

    expect_column_predicted_cardinality_to_be is a \
    [Column Aggregate Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations).

    Args:
        column (str): \
            The column name.
        cardinality_category str: \
            The expected cardinality category of the column. \
        depth: \
            The depth of the cardinality prediction. \
            If depth=1, then categories are: "INFINITE", "FINITE" \
            If depth=2, then categories are: "UNIQUE", "DUPLICATED", "A_FEW", "SEVERAL", "MANY" \

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        include_config (boolean): \
            If True, then include the expectation config as part of the result object.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, include_config, catch_exceptions, and meta.

    Notes:
        * This expectation is experimental and in active development. It may change in the future.
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

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """

        super().validate_configuration(configuration)
        configuration = configuration or self.configuration

        try:
            depth = configuration.kwargs.get("depth", 2)

            assert (
                depth in [1, 2]
            ), "depth must be 1 or 2"

            if depth == 1:
                assert (
                    configuration.kwargs["cardinality_category"] in [
                        "INFINITE",
                        "FINITE",
                    ]
                ), "With depth=1, cardinality_category must be INFINITE or FINITE"
            
            elif depth == 2:
                assert (
                    configuration.kwargs["cardinality_category"] in [
                        "INFINITE",
                        "FINITE",
                        "UNIQUE",
                        "DUPLICATED",
                        "A_FEW",
                        "SEVERAL",
                        "MANY",
                    ]
                ), "With depth=2, cardinality_category must be one of the following: INFINITE, FINITE, UNIQUE, DUPLICATED, A_FEW, SEVERAL, MANY"

        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
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