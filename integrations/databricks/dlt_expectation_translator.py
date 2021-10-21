from typing import List, Tuple

from great_expectations.core import ExpectationConfiguration
from integrations.databricks.exceptions import UnsupportedExpectationConfiguration

ENABLED_EXPECTATIONS: Tuple = (
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_be_between",
)


def translate_expectation_config_to_dlt_expectation(
    expectation_configuration: ExpectationConfiguration,
    dlt_expectation_name: str = None,
) -> List[Tuple]:
    """
    Translates Great Expectations ExpectationConfiguration objects to a list of Databricks Delta Live Table style expectation configurations
    Args:
        expectation_configuration: ExpectationConfiguration object, must be one of the ENABLED_EXPECTATIONS
        dlt_expectation_name: Optional name to be used with the Delta Live Table expectation

    Returns:
        List of (dlt_expectation_name, dlt-style condition)
    """

    if expectation_configuration.expectation_type not in ENABLED_EXPECTATIONS:
        raise UnsupportedExpectationConfiguration(
            f"The expectation type `{expectation_configuration.expectation_type}` of the ExpectationConfiguration you provided is not supported. Please provide one of the supported types: {ENABLED_EXPECTATIONS}"
        )

    # TODO: Move these into separate methods for each expectation type:
    if (
        expectation_configuration.expectation_type
        == "expect_column_values_to_not_be_null"
    ):
        return [
            (
                dlt_expectation_name,
                f"{expectation_configuration.get_domain_kwargs()['column']} IS NOT NULL",
            )
        ]

    if (
        expectation_configuration.expectation_type
        == "expect_column_values_to_be_between"
    ):

        return_list = []

        success_kwargs = expectation_configuration.get_success_kwargs()
        domain_kwargs = expectation_configuration.get_domain_kwargs()
        column_name = domain_kwargs["column"]

        min_value = success_kwargs.get("min_value")
        max_value = success_kwargs.get("max_value")

        if min_value is not None:
            if success_kwargs.get("strict_min"):
                min_conditional = ">"
            else:
                min_conditional = ">="

            string_repr_min = f"{column_name} {min_conditional} {str(min_value)}"

            return_list.append((f"{dlt_expectation_name}_1", string_repr_min))

        if max_value is not None:
            if success_kwargs.get("strict_max"):
                max_conditional = "<"
            else:
                max_conditional = "<="

            string_repr_max = f"{column_name} {max_conditional} {str(max_value)}"

            return_list.append((f"{dlt_expectation_name}_2", string_repr_max))

        return return_list
