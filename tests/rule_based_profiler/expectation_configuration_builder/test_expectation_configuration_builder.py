from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)


def test_build_expectation_configuration():
    expectation_configuration_builder = ExpectationConfigurationBuilder(
        expectation_type="expect_column_values_to_be_between"
    )

    expectation_configuration_builder.build_expectation_configuration()
