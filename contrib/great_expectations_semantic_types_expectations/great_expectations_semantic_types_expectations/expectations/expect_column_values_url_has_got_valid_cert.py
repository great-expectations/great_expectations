"""
This is a template for creating custom ColumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations
"""
import datetime
import json
import socket
import ssl
from datetime import datetime
from typing import Optional

from dateutil.parser import parse

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


def get_certificate_exp_date(host, port=443, timeout=1):
    try:
        context = ssl.create_default_context()
        conn = socket.create_connection((host, port))
        sock = context.wrap_socket(conn, server_hostname=host)
        sock.settimeout(timeout)
        cert = sock.getpeercert()
        expiry_date = datetime.strptime(cert["notAfter"], "%b %d %H:%M:%S %Y %Z")
        before_date = datetime.strptime(cert["notBefore"], "%b %d %H:%M:%S %Y %Z")
        return before_date, expiry_date
    except Exception as e:
        return parse("1900-01-01"), parse("1900-01-01")
    finally:
        sock.close()


def has_valid_cert(url: str) -> bool:
    try:
        before_date, expiry_date = get_certificate_exp_date(url)
        if (
            expiry_date > datetime.utcnow()
            and before_date < datetime.utcnow()
            and expiry_date is not None
            and before_date is not None
        ):
            return True
        else:
            return False
    except Exception as e:
        return False


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesUrlHasGotValidCert(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.valid_cert"

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.apply(lambda x: has_valid_cert(x))

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnValuesUrlHasGotValidCert(ColumnMapExpectation):
    """Expect provided url has got valid cert. (NotBefore < now < NotAfter"""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "all_valid": [
                    "google.com",
                    "index.hu",
                    "yahoo.com",
                    "github.com",
                    "microsoft.com",
                ],
                "some_other": [
                    "google.com",
                    "index.hu",
                    "yahoo.com",
                    "github.com",
                    "expired.badssl.com",
                ],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "all_valid"},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "some_other", "mostly": 0.9},
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.valid_cert"

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
            "hackathon-22",
            "experimental",
            "typed-entities",
        ],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@szecsip",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnValuesUrlHasGotValidCert().print_diagnostic_checklist()
