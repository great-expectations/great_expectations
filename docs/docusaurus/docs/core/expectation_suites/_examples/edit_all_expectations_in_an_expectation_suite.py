"""
This example script demonstrates how to edit all Expectations in an
 Expectation Suite.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""
# <snippet name="core/expectation_suites/_examples/edit_all_expectations_in_an_expectation_suite.py full example code">
# 1. Get the Expectation Suite containing the Expectations to edit.
# <snippet name="core/expectation_suites/_examples/edit_all_expectations_in_an_expectation_suite.py create and populate Expectation Suite">
import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core.expectation_suite import ExpectationSuite

context = gx.get_context()

suite = context.suites.add(suite=ExpectationSuite(name="my_expectation_suite"))

suite.add_expectation(
    gxe.ExpectColumnValuesToBeInSet(column="passenger_count", value_set=[1, 2, 3, 4, 5])
)
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="pickup_datetime"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="passenger_count"))
# </snippet>

# If you are editing an existing Expectation Suite, retrieve it with:
# <snippet name="core/expectation_suites/_examples/edit_all_expectations_in_an_expectation_suite.py get Expectation Suite">
existing_suite_name = (
    "my_expectation_suite"  # replace this with the name of your Expectation Suite
)
suite = context.suites.get(name=existing_suite_name)
# </snippet>

# 2. Iterate through the Expectation Suite's Expectations and modify their attributes.
# <snippet name="core/expectation_suites/_examples/edit_all_expectations_in_an_expectation_suite.py modify Expectations">
for expectation in suite.expectations:
    expectation.notes = "This Expectation was generated as part of GX Documentation."
# </snippet>

# 3. Save the Expectation Suite and all modifications to the Expectations within it.
# <snippet name="core/expectation_suites/_examples/edit_all_expectations_in_an_expectation_suite.py save Expectation Suite">
suite.save()
# </snippet>
# </snippet>
