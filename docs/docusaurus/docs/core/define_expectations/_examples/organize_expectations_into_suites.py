"""
This is an example script for how to organize Expectations into Expectation Suites.

To test, run:
pytest --docs-tests -k "doc_example_organize_expectations_into_expectation_suites" tests/integration/test_script_runner.py
"""


def set_up_context_for_example(context):
    pass


# EXAMPLE SCRIPT STARTS HERE:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/organize_expectations_into_suites.py - full code example">
import great_expectations as gx

context = gx.get_context()
# Hide this
set_up_context_for_example(context)

# Create an Expectation Suite
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/organize_expectations_into_suites.py - create an Expectation Suite">
suite_name = "my_expectation_suite"
suite = gx.ExpectationSuite(name=suite_name)
# </snippet>

# Add the Expectation Suite to the Data Context
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/organize_expectations_into_suites.py - add Expectation Suite to the Data Context">
suite = context.suites.add(suite)
# </snippet>

# Create an Expectation to put into an Expectation Suite
expectation = gx.expectations.ExpectColumnValuesToNotBeNull(column="passenger_count")

# Add the previously created Expectation to the Expectation Suite
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/organize_expectations_into_suites.py - add an Expectation in a variable to an Expectation Suite">
suite.add_expectation(expectation)
# </snippet>

# Add another Expectation to the Expectation Suite.
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="pickup_datetime")
)

# Update the configuration of an Expectation, then push the changes to the Expectation Suite
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/organize_expectations_into_suites.py - update an Expectation and push changes to the Suite config">
expectation.column = "pickup_location_id"
expectation.save()
# </snippet>

# Retrieve an Expectation Suite from the Data Context
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/organize_expectations_into_suites.py - retrieve an Expectation Suite">
existing_suite_name = (
    "my_expectation_suite"  # replace this with the name of your Expectation Suite
)
suite = context.suites.get(name=existing_suite_name)
# </snippet>
# </snippet>
