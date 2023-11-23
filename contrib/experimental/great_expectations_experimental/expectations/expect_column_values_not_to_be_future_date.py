"""
This is a template for creating custom ColumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations
"""
from datetime import date
from typing import Optional

from dateutil.parser import parse

from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


def is_not_a_future_date(date_in: str) -> bool:
    try:
        today = date.today()
        if isinstance(date_in, str):
            d = parse(date_in)
        else:
            d = date_in
        d = d.date()
        if d > today:
            return False
        else:
            return True
    except Exception:
        return False


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesNotToBeFutureDate(ColumnMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.valid_date"

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.apply(lambda x: is_not_a_future_date(x))

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, _execution_engine, **kwargs):
        query = sa.select(column).select_from(kwargs["_table"])
        results = _execution_engine.execute_query(query).fetchall()
        date_strings = [date_tuple[0] for date_tuple in results]
        answer = [is_not_a_future_date(date_str) for date_str in date_strings]
        result = all(answer)
        return result

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnValuesNotToBeFutureDate(ColumnMapExpectation):
    """Expect column values not to be the future date."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "all_valid": [
                    "2022-01-01",
                    "2022-01-02",
                    "2022/03/03",
                    "01/01/2022",
                    "2022.01.01",
                ],
                "some_other": [
                    "2022-02-30",
                    "2022-11-31",
                    "2022/03/30",
                    "2222/01/02",
                    "2322.01.01",
                ],
                "another_test": [
                    "2022-02-28",
                    "2022-11-11",
                    "2022/03/30",
                    "2002/01/02",
                    "2322.01.01",
                ],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    # this is being called in sqlite
                    "in": {"column": "all_valid"},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "some_other", "mostly": 1},
                    "out": {
                        "success": False,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "another_test", "mostly": 1},
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.valid_date"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("mostly",)

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

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
        if configuration is None:
            configuration = self.configuration

        # # Check other things in configuration.kwargs and raise Exceptions if needed
        # try:
        #     assert (
        #         ...
        #     ), "message"
        #     assert (
        #         ...
        #     ), "message"
        # except AssertionError as e:
        #     raise InvalidExpectationConfigurationError(str(e))
        return True

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "maturity": "experimental",
        "tags": [
            "experimental",
            "typed-entities",
        ],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@prachijain136",  # Don't forget to add your github handle here!
            "@mcornew",
            "@m-Chetan",
            "@Pritampyaare",
        ],
    }


if __name__ == "__main__":
    ExpectColumnValuesNotToBeFutureDate().print_diagnostic_checklist()
