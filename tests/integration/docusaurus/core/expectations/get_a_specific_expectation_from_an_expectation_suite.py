"""
This example script demonstrates how to get a specific Expectation from an
 Expectation Suite.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""
# <snippet name="tests/integration/docusaurus/core/expectations/get_a_specific_expectation_from_an_expectation_suite.py full example code">
# 1. Import the GX Core library and `expectations` module.
# <snippet name="tests/integration/docusaurus/core/expectations/get_a_specific_expectation_from_an_expectation_suite.py imports">
import great_expectations as gx
import great_expectations.expectations as gxe

# </snippet>
# This section creates a new Expectation Suite, which will later have Expectations
# added to it.
# Disregard this code if you are retrieving an existing Expectation Suite..
from great_expectations.core.expectation_suite import ExpectationSuite

new_suite_name = "my_empty_expectation_suite"
new_suite = ExpectationSuite(name=new_suite_name)

# 2. Get a Data Context
# <snippet name="tests/integration/docusaurus/core/expectations/get_a_specific_expectation_from_an_expectation_suite.py get_context">
context = gx.get_context()
# </snippet>

# 3. Create a new or retrieve an existing Expectation Suite
# This section adds the Expectation Suite created earlier to the Data Context.
#  Disregard this line if the Expectation Suite you are retrieving has already
#  been added to your Data Context.
suite = context.suites.add(new_suite)

# To retrieve an existing suite, use the following:
existing_suite_name = (
    "my_empty_expectation_suite"  # replace this with the name of your Expectation Suite
)
suite = context.suites.get(name=existing_suite_name)

# This section adds some Expectations to the Expectations Suite.
# Later the second of these will be retrieved from the Expectation Suite.
suite.add_expectation(
    gxe.ExpectColumnValuesToBeInSet(column="passenger_count", value_set=[1, 2, 3, 4, 5])
)
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="pickup_datetime"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="passenger_count"))

# 4. Iterate through the Expectation Suite's Expectations and check
# Expectation class and attributes until the desired Expectation is found.
# <snippet name="tests/integration/docusaurus/core/expectations/get_a_specific_expectation_from_an_expectation_suite.py retrieve expectation">
expectation = next(
    expectation
    for expectation in suite.expectations
    if isinstance(expectation, gxe.ExpectColumnValuesToNotBeNull)
    and expectation.column == "pickup_datetime"
)
# </snippet>
# </snippet>
