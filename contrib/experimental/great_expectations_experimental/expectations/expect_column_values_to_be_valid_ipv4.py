import ipaddress
from typing import Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


# This method compares a string to the valid IPv4 address format
def is_valid_ipv4(addr: str) -> bool:
    try:
        ipaddress.IPv4Address(addr)
        return True
    except ipaddress.AddressValueError:
        return False


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesToBeValidIPv4(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.valid_ipv4"

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.apply(lambda x: is_valid_ipv4(x))

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnValuesToBeValidIpv4(ColumnMapExpectation):
    """This Expectation validates data as conforming to the valid IPv4 address format."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "well_formed_ipv4": [
                    # decimal-dot notation IPv4 addresses
                    "0.0.0.0",
                    "192.168.0.1",
                    "255.255.255.255",
                    # integer fits into 32 bits
                    0,
                    3232235521,
                    4294967295,
                    # integer packed into a bytes object of length 4
                    b"\x00\x00\x00\x00",
                    b"\xC0\xA8\x00\x01",
                    b"\xFF\xFF\xFF\xFF",
                ],
                "malformed_ipv4": [
                    # has non-decimal value
                    "192.168.0.1a",
                    # too short decimal-dot notation
                    "192.168.0.",
                    # too long decimal-dot notation
                    "192.168.0.1.1",
                    # invalid format
                    "192-168-0-1",
                    # IPv4 addresses over range
                    "255.255.255.256",
                    -1,
                    4294967296,
                    # left field
                    "not-even-address",
                    "ValueError('All arrays must be of the same length')",
                ],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "well_formed_ipv4"},
                    "out": {"success": True},
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "malformed_ipv4"},
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.valid_ipv4"

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

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "maturity": "experimental",
        "tags": ["experimental", "hackathon", "typed-entities"],
        "contributors": [
            "@voidforall",
        ],
    }


if __name__ == "__main__":
    ExpectColumnValuesToBeValidIpv4().print_diagnostic_checklist()
