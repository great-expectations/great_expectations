"""
This is a template for creating custom ColumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations
"""
import json
from typing import Optional

import blockcypher
import requests

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


def is_btc_tx_confirmed(tx: str) -> bool:
    try:
        cnt = blockcypher.get_num_confirmations(tx)
        if cnt > 0:
            return True
        else:
            return False
    except Exception as e:
        print(e)
        return False


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesBitcoinTxIsConfirmed(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.bitcoin_tx_is_confirmed"

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.apply(lambda x: is_btc_tx_confirmed(x))

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnValuesBitcoinTxIsConfirmed(ColumnMapExpectation):
    """Expect column values Bitcoin transaction hash is confirmed"""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "all_valid": [
                    "7f0881d44ad2c5972457825c0846ba330a53b159251e3f53a7d0de4cd2727590",
                    "4eeeaf8216f8ba3c5258dc1d4fc489a35381dda8259e5160d885b851e21e319d",
                    "6d12598311328a63c324ee7164b6d97e8d38b6af42cd4017643602fd982cc40c",
                    "ac6080a633e4bcf39e437ba160c95c8edfbb9fabe3525058a7fca2d890cc4ef5",
                ],
                "some_other": [
                    "1BoatSLRHtKNngkdXEeobR76b53LETtpyT",
                    "n2nzi7xDTrMVK9stGpbK3BtrpBCJfH7LRQ",
                    "3QJmV3qfvL9SuYo34YihAf3sRCW3qSinyC",
                    "bc1qxneu85dnhx33asv8da45x55qyeu44ek9h3vngxdsare",
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
                    "in": {"column": "some_other", "mostly": 1},
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.bitcoin_tx_is_confirmed"

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
        "requirements": ["blockcypher"],
    }


if __name__ == "__main__":
    ExpectColumnValuesBitcoinTxIsConfirmed().print_diagnostic_checklist()
