"""
This example script is intended to demonstrate how you can determine if an Expectation works under the self-initializing
framework.
"""
# The following is the example code that will be displayed in documentation.

from great_expectations.expectations.expectation import Expectation

Expectation.is_expectation_self_initializing(name="expect_column_to_exist")

Expectation.is_expectation_self_initializing(name="expect_column_mean_to_be_between")


# NOTE: The following assertions are only for testing and can be ignored by users.
assert (
    Expectation.is_expectation_self_initializing(name="expect_column_to_exist") is False
)
assert (
    Expectation.is_expectation_self_initializing(
        name="expect_column_mean_to_be_between"
    )
    is True
)
