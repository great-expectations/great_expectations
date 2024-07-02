"""
This example script demonstrates how to edit a single Expectation in an
 Expectation Suite.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""

# <snippet name="core/expectation_suites/_examples/edit_a_single_expectation.py full example code">
import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core.expectation_suite import ExpectationSuite

context = gx.get_context()

new_suite_name = "my_expectation_suite"
new_suite = ExpectationSuite(name=new_suite_name)
context.suites.add(new_suite)

new_suite.add_expectation(
    gxe.ExpectColumnValuesToBeInSet(column="passenger_count", value_set=[1, 2, 3, 4, 5])
)
new_suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="pickup_datetime"))
new_suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="passenger_count"))

existing_suite_name = (
    "my_expectation_suite"  # replace this with the name of your Expectation Suite
)
suite = context.suites.get(name=existing_suite_name)
expectation = next(
    expectation
    for expectation in suite.expectations
    if isinstance(expectation, gxe.ExpectColumnValuesToNotBeNull)
    and expectation.column == "pickup_datetime"
)

# highlight-start
# <snippet name="core/expectation_suites/_examples/edit_a_single_expectation.py edit attribute">
expectation.column = "pickup_location_id"
# </snippet>
# highlight-end

# highlight-start
# <snippet name="core/expectation_suites/_examples/edit_a_single_expectation.py save the Expectation">
expectation.save()
# </snippet>
# highlight-end
# </snippet>
