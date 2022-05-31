"""Example Script: How to determine if an Expectation is self-initializing

This example script is intended for use in online documentation that shows how a user can determine if an Expectation
will work within the self-initializing framework.

Assert statements are included to ensure that if the behaviour shown in this script breaks it will not pass
tests and will be updated.  These statements can be ignored by users.

Inline comments with the tags `<snippet>` and `</snippet>` are used to ensure that if this script is updated
the snippets that are specified for use in documentation are maintained.  These comments can be ignored by users.

<relevant documentation>
    https://docs.greatexpectations.io/docs/
"""
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
