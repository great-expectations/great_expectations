"""
This example script demonstrates how to edit all Expectations in an
 Expectation Suite.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""
# <snippet name="tests/integration/docusaurus/core/expectations/edit_all_expectations_in_an_expectation_suite.py full example code">
# The following sections set up an Expectation Suite with Expectations
# that can later be retrieved and edited as an example.
import great_expectations as gx
import great_expectations.expectations as gxe

# This section creates a new Expectation Suite, which will later have Expectations
# added to it.
# Disregard this code if you are retrieving an existing Expectation Suite..
from great_expectations.core.expectation_suite import ExpectationSuite

new_suite_name = "my_empty_expectation_suite"
new_suite = ExpectationSuite(name=new_suite_name)

context = gx.get_context()

# This section adds the Expectation Suite created earlier to the Data Context.
# Disregard this line if the Expectation Suite you are retrieving has already
# been added to your Data Context.
suite = context.suites.add(new_suite)

# This section adds some Expectations to the Expectations Suite.
# Later these will be modified.
suite.add_expectation(
    gxe.ExpectColumnValuesToBeInSet(column="passenger_count", value_set=[1, 2, 3, 4, 5])
)
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="pickup_datetime"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="passenger_count"))

# 1. Get the Expectation Suite containing the Expectations to edit.
# If you are editing an existing Expectation Suite, retrieve it with:
existing_suite_name = (
    "my_empty_expectation_suite"  # replace this with the name of your Expectation Suite
)
suite = context.suites.get(name=existing_suite_name)

# 2. Iterate through the Expectation Suite's Expectations and modify their attributes.
# <snippet name="tests/integration/docusaurus/core/expectations/edit_all_expectations_in_an_expectation_suite.py modify Expectations">
for expectation in suite.expectations:
    expectation.notes = "This Expectation was generated as part of GX Documentation."
# </snippet>

# 3. Save the Expectation Suite and all modifications to the Expectations within it.
# <snippet name="tests/integration/docusaurus/core/expectations/edit_all_expectations_in_an_expectation_suite.py save Expectation Suite">
suite.save()
# </snippet>
# </snippet>
