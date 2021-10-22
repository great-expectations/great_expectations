from typing import List, Tuple

import sqlparse

from great_expectations.core import ExpectationConfiguration
from integrations.databricks.exceptions import (
    UnsupportedDLTExpectationConfiguration,
    UnsupportedExpectationConfiguration,
)

ENABLED_EXPECTATIONS_GE_TO_DLT: Tuple = (
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_be_between",
)

ENABLED_EXPECTATIONS_DLT_TO_GE = ("expect_column_values_to_not_be_null",)


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

    if expectation_configuration.expectation_type not in ENABLED_EXPECTATIONS_GE_TO_DLT:
        raise UnsupportedExpectationConfiguration(
            f"The expectation type `{expectation_configuration.expectation_type}` of the ExpectationConfiguration you provided is not supported. Please provide one of the supported types: {ENABLED_EXPECTATIONS_GE_TO_DLT}"
        )

    # TODO: Move these into separate methods for each expectation type:
    if (
        expectation_configuration.expectation_type
        == "expect_column_values_to_not_be_null"
    ):
        return [
            (
                dlt_expectation_name,
                # TODO: Handle columns that require quoting
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

            # return_list.append((f"{dlt_expectation_name}_1", string_repr_min))

        if max_value is not None:
            if success_kwargs.get("strict_max"):
                max_conditional = "<"
            else:
                max_conditional = "<="

            string_repr_max = f"{column_name} {max_conditional} {str(max_value)}"

            # return_list.append((f"{dlt_expectation_name}_2", string_repr_max))

        # TODO: return can be a single item if we can AND two conditionals
        if min_value is not None and max_value is not None:
            return_list.append(
                (dlt_expectation_name, f"{string_repr_min} AND {string_repr_max}")
            )
        elif min_value is not None:
            return_list.append((dlt_expectation_name, f"{string_repr_min}"))
        elif max_value is not None:
            return_list.append((dlt_expectation_name, f"{string_repr_max}"))

        return return_list


def translate_dlt_expectation_to_expectation_config(
    dlt_expectations: List[Tuple[str, str]], ge_expectation_type: str
) -> ExpectationConfiguration:
    """
    Only works for simple comparisons of two items e.g. a >= 10
    Args:
        dlt_expectations: a single or pair of dlt_expectations to convert to Great Expectations ExpectationConfigurations

    Returns:
        Great Expectations ExpectationConfiguration objects
    """
    # TODO: In the future we can infer the GE expectation type
    if ge_expectation_type not in ENABLED_EXPECTATIONS_DLT_TO_GE:
        raise UnsupportedExpectationConfiguration(
            f"The expectation type `{ge_expectation_type}` is not supported. Please provide one of the supported types: {ENABLED_EXPECTATIONS_DLT_TO_GE}"
        )

    if ge_expectation_type == "expect_column_values_to_not_be_null":
        if len(dlt_expectations) > 1:
            raise UnsupportedDLTExpectationConfiguration(
                f"For this expectation type {ge_expectation_type}, please submit a list of only one dlt_expectation."
            )

        dlt_expectation_name: str = dlt_expectations[0][0]
        dlt_expectation_comparison: str = dlt_expectations[0][1]

        if not "IS NOT NULL" in dlt_expectation_comparison.upper():
            raise UnsupportedDLTExpectationConfiguration(
                f"For this expectation type {ge_expectation_type}, please submit an expectation containing an IS NOT NULL clause."
            )

        column_name: str = sqlparse.parse(dlt_expectation_comparison)[0].tokens[0].value

        return ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={
                "column": column_name,
                "result_format": "COMPLETE",
            },
            meta={"notes": f"DLT expectation name: {dlt_expectation_name}"},
        )

    # if ge_expectation_type == "expect_column_values_to_be_between":
    #
    #     for dlt_expectation in dlt_expectations:
    #
    #         dlt_expectation_name = dlt_expectation[0]
    #         dlt_expectation_value = dlt_expectation[1]
    #
    #         comparison = sqlparse.parse(dlt_expectation_value)[0].tokens[0]
    #
    #         column = comparison.left
    #         value = comparison.right
    #         # This is very brittle
    #         operator = comparison[1]
    #
    #
    #     if operator == ">=":
    #         expectation_configuration = ExpectationConfiguration(
    #         expectation_type="expect_column_values_to_be_between",
    #         kwargs={
    #             "column": column,
    #             "min_value": float(value),
    #             "max_value": None,
    #             "strict_min": False,
    #             "strict_max": False,
    #             "result_format": "COMPLETE",
    #         },
    #         meta={"notes": f"DLT expectation name: {dlt_expectation_name}"},
    #     )
