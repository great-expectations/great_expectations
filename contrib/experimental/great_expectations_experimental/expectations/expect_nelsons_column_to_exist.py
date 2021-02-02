from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import TableExpectation


class ExpectNelsonsColumnToExist(TableExpectation):
    """Return true, with some fun quips in the details.

    expect_nelsons_column_to_exist is a :func:`expectation \
    <great_expectations.validator.validator.Validator.expectation>`. It is designed
    to show how it is possible to access experimental Expectations.

    Returns:
        ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.
    """

    metric_dependencies = tuple()
    success_keys = tuple()
    domain_keys = (
        "batch_id",
        "table",
    )

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration:
                An optional Expectation Configuration entry that will be used to configure the expectation

        Returns:
            True if the configuration has been validated successfully. Otherwise, raises an exception
        """

        # Setting up a configuration
        super().validate_configuration(configuration)
        return True

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        return {
            "success": True,
            "result": {
                "details": {
                    "dickens_say": "Contributing to open source makes the world a better place."
                }
            },
        }
