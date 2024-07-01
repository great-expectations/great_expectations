"""
This example script demonstrates how to get a specific Expectation from an
 Expectation Suite.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""

# <snippet name="core/expectation_suites/_examples/get_a_specific_expectation_from_an_expectation_suite.py full example code">
import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core.expectation_suite import ExpectationSuite

context = gx.get_context()

suite = context.suites.add(ExpectationSuite(name="my_expectation_suite"))
suite.add_expectation(
    gxe.ExpectColumnValuesToBeInSet(column="passenger_count", value_set=[1, 2, 3, 4, 5])
)
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="pickup_datetime"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="passenger_count"))

# highlight-start
# <snippet name="core/expectation_suites/_examples/get_a_specific_expectation_from_an_expectation_suite.py retrieve expectation">
expectation = next(
    expectation
    for expectation in suite.expectations
    if isinstance(expectation, gxe.ExpectColumnValuesToNotBeNull)
    and expectation.column == "pickup_datetime"
)
# </snippet>
# highlight-end
# </snippet>
