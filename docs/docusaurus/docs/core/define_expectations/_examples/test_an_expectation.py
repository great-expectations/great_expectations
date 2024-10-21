"""
This is an example script for how to test an individual Expectation against a Batch of sample data.

To test, run:
pytest --docs-tests -k "doc_example_test_an_expectation" tests/integration/test_script_runner.py
"""


def set_up_context_for_example(context):
    pass


# EXAMPLE SCRIPT STARTS HERE:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/test_an_expectation.py - full code example">
import great_expectations as gx

context = gx.get_context()
# Hide this
set_up_context_for_example(context)


# Use the `pandas_default` Data Source to retrieve a Batch of sample Data from a data file:
file_path = "./data/folder_with_data/yellow_tripdata_sample_2019-01.csv"
batch = context.data_sources.pandas_default.read_csv(file_path)

# Define the Expectation to test:
expectation = gx.expectations.ExpectColumnMaxToBeBetween(
    column="passenger_count", min_value=1, max_value=6
)

# Test the Expectation:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/test_an_expectation.py - test expectation with preset parameters">
validation_results = batch.validate(expectation)
# </snippet>

# Evaluate the Validation Results:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/test_an_expectation.py - evaluate Validation Results">
print(validation_results)
# </snippet>

# If needed, adjust the Expectation's preset parameters and test again:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/test_an_expectation.py - modify preset expectation parameters">
expectation.min_value = 1
expectation.max_value = 6
# </snippet>

# Test the modified expectation and review the new Validation Results:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/test_an_expectation.py - test and review a modified Expectation">
new_validation_results = batch.validate(expectation)
print(new_validation_results)
# </snippet>
# </snippet>


# Alternatively, define an Expectation that uses an Expectation Parameter dictionary:
expectation = gx.expectations.ExpectColumnMaxToBeBetween(
    column="passenger_count",
    min_value={"$PARAMETER": "expect_passenger_max_to_be_above"},
    max_value={"$PARAMETER": "expect_passenger_max_to_be_below"},
)

# Define the Expectation Parameter values and test the Expectation:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/test_an_expectation.py - test expectation with expectation parameters">
runtime_expectation_parameters = {
    "expect_passenger_max_to_be_above": 4,
    "expect_passenger_max_to_be_below": 6,
}
validation_results = batch.validate(
    expectation, expectation_parameters=runtime_expectation_parameters
)
# </snippet>

# Evaluate the Validation Results:

print(validation_results)


# If needed, update the Expectation Parameter dictionary and test again:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/test_an_expectation.py - modify and retest Expectation Parameters dictionary">
runtime_expectation_parameters = {
    "expect_passenger_max_to_be_above": 1,
    "expect_passenger_max_to_be_below": 6,
}
validation_results = batch.validate(
    expectation, expectation_parameters=runtime_expectation_parameters
)
print(validation_results)
# </snippet>
