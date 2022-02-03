import uuid
from typing import Optional, Tuple

from great_expectations.core import ExpectationConfiguration
from integrations.databricks.dlt_expectation_translator import (
    translate_dlt_expectation_to_expectation_config,
    translate_expectation_config_to_dlt_expectation,
)


class DLTExpectation:
    def __init__(self, name: str, condition: str):

        self._name = name
        self._condition = condition

    @property
    def name(self) -> str:
        """Name of the DLT expectation"""
        return self._name

    @property
    def condition(self) -> str:
        """Condition of the DLT expectation"""
        return self._condition

    def to_great_expectations_expectation(self) -> ExpectationConfiguration:

        # TODO: this method body is a placeholder, this should be inferred and a seprate parser
        #  dispatched from a registry and used to handle conversion
        ge_expectation_type: str = "expect_column_values_to_not_be_null"

        ge_expectation_configuration: ExpectationConfiguration = (
            translate_dlt_expectation_to_expectation_config(
                dlt_expectations=[(self._name, self._condition)],
                ge_expectation_type=ge_expectation_type,
            )
        )

        return ge_expectation_configuration


class DLTExpectationFactory:
    def from_great_expectations_expectation(
        self,
        ge_expectation_configuration: ExpectationConfiguration,
        dlt_expectation_name: Optional[str] = None,
    ) -> DLTExpectation:
        """Factory Method - Construct a DLTExpectation object from GE Expectation Configuration"""

        if dlt_expectation_name is None:
            dlt_expectation_name = (
                self._default_dlt_expectation_name_from_ge_expectation_configuration(
                    ge_expectation_configuration=ge_expectation_configuration
                )
            )

        # TODO: this method body is a placeholder, this should be inferred and a seprate parser
        #  dispatched from a registry and used to handle conversion
        dlt_expectation_tuple: Tuple[
            str, str
        ] = translate_expectation_config_to_dlt_expectation(
            expectation_configuration=ge_expectation_configuration,
            dlt_expectation_name=dlt_expectation_name,
        )[
            0
        ]
        dlt_expectation: DLTExpectation = DLTExpectation(
            name=dlt_expectation_tuple[0], condition=dlt_expectation_tuple[1]
        )
        return dlt_expectation

    @staticmethod
    def _default_dlt_expectation_name_from_ge_expectation_configuration(
        ge_expectation_configuration: ExpectationConfiguration,
    ) -> str:
        dlt_expectation_name: str = (
            f"{ge_expectation_configuration.expectation_type}_{uuid.uuid4().hex[:8]}"
        )
        return dlt_expectation_name
