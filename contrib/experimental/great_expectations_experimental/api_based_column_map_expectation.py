from abc import ABC
from typing import Optional

from great_expectations.exceptions.exceptions import (
    InvalidExpectationConfigurationError,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    ExpectationConfiguration,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.util import camel_to_snake


class APIColumnMapMetricProvider(ColumnMapMetricProvider):
    condition_value_keys = ()

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.apply(
            lambda x: cls.request_func_(
                cls.endpoint_,
                cls.method_,
                cls.header_,
                cls.body_,
                cls.auth_,
                cls.data_key_,
                cls.result_key_,
                x,
            )
        )


class APIBasedColumnMapExpectation(ColumnMapExpectation, ABC):
    @staticmethod
    def register_metric(
        api_camel_name: str,
        endpoint_: str,
        method_: str = None,
        header_=None,
        body_=None,
        auth_=None,
        data_key_=None,
        result_key_=None,
        request_func_=None,
    ):
        api_snake_name = camel_to_snake(api_camel_name)
        map_metric = "column_values.match_" + api_snake_name + "_api"

        # Define the class using `type`. This allows us to name it dynamically.
        new_column_api_metric_provider = type(
            f"(ColumnValuesMatch{api_camel_name}API",
            (APIColumnMapMetricProvider,),
            {
                "condition_metric_name": map_metric,
                "endpoint_": endpoint_,
                "method_": method_,
                "header_": header_,
                "body_": body_,
                "auth_": auth_,
                "data_key_": data_key_,
                "result_key_": result_key_,
                "request_func_": request_func_,
            },
        )

        return map_metric

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        super().validate_configuration(configuration)
        try:
            assert (
                getattr(self, "endpoint_", None) is not None
            ), "endpoint_ is required for APIBasedColumnMap Expectations"

            assert (
                "column" in configuration.kwargs
            ), "'column' parameter is required for ColumnMap expectations"

            if "mostly" in configuration.kwargs:
                mostly = configuration.kwargs["mostly"]
                assert isinstance(
                    mostly, (int, float)
                ), "'mostly' parameter must be an integer or float"
                assert 0 <= mostly <= 1, "'mostly' parameter must be between 0 and 1"

        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
