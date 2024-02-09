"""
This example script demonstrates how to edit a single Expectation in an
 Expectation Suite.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""
# <snippet name="core/expectation_suites/_examples/edit_a_single_expectation.py full example code">
# The following sections set up an Expectation Suite with Expectations
# that can later be retrieved and edited as an example.
# <snippet name="core/expectation_suites/_examples/edit_a_single_expectation.py imports">
import great_expectations as gx
import great_expectations.expectations as gxe

# </snippet>
# This section creates a new Expectation Suite, which will later have Expectations
# added to it.
# Disregard this code if you are retrieving an existing Expectation Suite..
from great_expectations.core.expectation_suite import ExpectationSuite

new_suite_name = "my_empty_expectation_suite"
new_suite = ExpectationSuite(name=new_suite_name)

# <snippet name="core/expectation_suites/_examples/edit_a_single_expectation.py get data context">
context = gx.get_context()
# </snippet>

# This section adds the Expectation Suite created earlier to the Data Context.
# Disregard this line if the Expectation Suite you are retrieving has already
# been added to your Data Context.
suite = context.suites.add(new_suite)

# This section adds some Expectations to the Expectations Suite.
# Later the second of these will be retrieved from the Expectation Suite and edited.
suite.add_expectation(
    gxe.ExpectColumnValuesToBeInSet(column="passenger_count", value_set=[1, 2, 3, 4, 5])
)
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="pickup_datetime"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="passenger_count"))

# 1. Get the Expectation to edit from an Expectation Suite.
# <snippet name="core/expectation_suites/_examples/edit_a_single_expectation.py get expectation to edit">
existing_suite_name = (
    "my_empty_expectation_suite"  # replace this with the name of your Expectation Suite
)
suite = context.suites.get(name=existing_suite_name)

expectation = next(
    expectation
    for expectation in suite.expectations
    if isinstance(expectation, gxe.ExpectColumnValuesToNotBeNull)
    and expectation.column == "pickup_datetime"
)
# </snippet>

# 2. Modify the Expectation's attributes.
# <snippet name="core/expectation_suites/_examples/edit_a_single_expectation.py edit attribute">
expectation.column = "pickup_location_id"
# </snippet>

# 3. Save the Expectation.
# <snippet name="core/expectation_suites/_examples/edit_a_single_expectation.py save the Expectation">
expectation.save()
# </snippet>
# </snippet>
