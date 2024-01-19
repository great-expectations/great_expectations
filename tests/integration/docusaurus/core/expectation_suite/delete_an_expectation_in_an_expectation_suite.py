"""
This example script demonstrates how to delete a specific Expectation in an
 Expectation Suite.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""
# <snippet name="tests/integration/docusaurus/core/expectation_suite/delete_an_expectation_in_an_expectation_suite.py full example code">

# 1. Import the Great Expectations library and `expectations` module.
# <snippet name="tests/integration/docusaurus/core/expectation_suite/delete_an_expectation_in_an_expectation_suite.py imports">
import great_expectations as gx
import great_expectations.expectations as gxe

# </snippet>
# This section creates a new Expectation Suite and adds some Expectations to it.
# Later, the this Expectation Suite will be retrieved and the second
# Expectation will be deleted.
# Disregard this code if you are retrieving an existing Expectation Suite.
from great_expectations.core.expectation_suite import ExpectationSuite

new_suite = ExpectationSuite(name="my_expectation_suite")
new_suite.add_expectation(
    gxe.ExpectColumnValuesToBeInSet(column="passenger_count", value_set=[1, 2, 3, 4, 5])
)
new_suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="pickup_datetime"))
new_suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="passenger_count"))

# 2. Get a Data Context
# <snippet name="tests/integration/docusaurus/core/expectation_suite/delete_an_expectation_in_an_expectation_suite.py get context">
context = gx.get_context()
# </snippet>

# This section adds the Expectation Suite created earlier to the Data Context.
# Disregard this line if the Expectation Suite you are retrieving has already
# been added to your Data Context.
suite = context.suites.add(new_suite)

# 3. Get the Expectation Suite containing the Expectation to delete.
# <snippet name="tests/integration/docusaurus/core/expectation_suite/delete_an_expectation_in_an_expectation_suite.py get Expectation Suite">
existing_suite_name = (
    "my_expectation_suite"  # replace this with the name of your Expectation Suite
)
suite = context.suites.get(name=existing_suite_name)
# </snippet>

# 4. Get the Expectation to delete.
# <snippet name="tests/integration/docusaurus/core/expectation_suite/delete_an_expectation_in_an_expectation_suite.py get Expectation">
expectation_to_delete = next(
    expectation
    for expectation in suite.expectations
    if isinstance(expectation, gxe.ExpectColumnValuesToNotBeNull)
    and expectation.column == "pickup_datetime"
)
# </snippet>

# 5. Use the Expectation Suite to delete the Expectation.
# <snippet name="tests/integration/docusaurus/core/expectation_suite/delete_an_expectation_in_an_expectation_suite.py delete the Expectation">
suite.delete_expectation(expectation=expectation_to_delete)
# </snippet>
# </snippet>
