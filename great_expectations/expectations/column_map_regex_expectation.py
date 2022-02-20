from abc import ABC
import json

from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    ExpectationConfiguration,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.metrics.util import get_dialect_regex_expression

class ColumnMapRegexMetricProvider(ColumnMapMetricProvider):
    condition_value_keys = ()

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.astype(str).str.contains(cls.regex)

    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     regex_expression = get_dialect_regex_expression(column, cls.regex, _dialect)
    #     if regex_expression is None:
    #         logger.warning(
    #             "Regex is not supported for dialect %s" % str(_dialect.dialect.name)
    #         )
    #         raise NotImplementedError

    #     return regex_expression

    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     return column.rlike(cls.regex)

class ColumnMapRegexExpectation(ColumnMapExpectation, ABC):

    @staticmethod
    def _register_metric(
        regex_snake_name : str,
        regex_camel_name : str,
        regex_ : str,
    ):
        map_metric = "column_values.match_"+regex_snake_name+"_regex"

        # Define the class using `type`. This allows us to name it dynamically.
        NewColumnRegexMetricProvider = type(
            f"(ColumnValuesMatch{regex_camel_name}Regex",
            (ColumnMapRegexMetricProvider,),
            {
                "condition_metric_name": map_metric,
                "regex": regex_,
            }
        )

        return map_metric

    # def validate_configuration(self, configuration: ExpectationConfiguration):
    #     pass
        # if not super().validate_configuration(configuration):
        #     return False
        # try:
        #     assert (
        #         "column" in configuration.kwargs
        #     ), "'column' parameter is required for column map expectations"
        #     if "mostly" in configuration.kwargs:
        #         mostly = configuration.kwargs["mostly"]
        #         assert isinstance(
        #             mostly, (int, float)
        #         ), "'mostly' parameter must be an integer or float"
        #         assert 0 <= mostly <= 1, "'mostly' parameter must be between 0 and 1"
        # except AssertionError as e:
        #     raise InvalidExpectationConfigurationError(str(e))
        # return True