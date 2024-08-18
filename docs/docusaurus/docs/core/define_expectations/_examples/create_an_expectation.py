"""
This is an example script for creating an expectation with preset or runtime parameters.

To test, run:
pytest --docs-tests -k "doc_example_create_an_expectation" tests/integration/test_script_runner.py
"""


def set_up_context_for_example(context):
    pass


# EXAMPLE SCRIPT STARTS HERE:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/create_an_expectation.py - full code example">
import great_expectations as gx

context = gx.get_context()
# Hide this
set_up_context_for_example(context)

# All Expectations are found in the `gx.expectations` module.
# This Expectation has all values set in advance:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/create_an_expectation.py - preset expectation">
preset_expectation = gx.expectations.ExpectColumnMaxToBeBetween(
    column="passenger_count", min_value=1, max_value=6
)
# </snippet>

# In this case, two Expectations are created that will be passed
#  parameters at runtime, and unique lookups are defined for each
#  Expectations' parameters.

# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/create_an_expectation.py - dynamic expectations">
passenger_expectation = gx.expectations.ExpectColumnMaxToBeBetween(
    column="passenger_count",
    min_value={"$PARAMETER": "expect_passenger_max_to_be_above"},
    max_value={"$PARAMETER": "expect_passenger_max_to_be_below"},
)
fare_expectation = gx.expectations.ExpectColumnMaxToBeBetween(
    column="fare",
    min_value={"$PARAMETER": "expect_fare_max_to_be_above"},
    max_value={"$PARAMETER": "expect_fare_max_to_be_below"},
)
# </snippet>

# A dictionary containing the parameters for both of the above
#   Expectations would look like:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/create_an_expectation.py - example expectation_parameters">
runtime_expectation_parameters = {
    "expect_passenger_max_to_be_above": 4,
    "expect_passenger_max_to_be_below": 6,
    "expect_fare_max_to_be_above": 10.00,
    "expect_fare_max_to_be_below": 500.00,
}
# </snippet>
# </snippet>
