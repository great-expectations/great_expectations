"""
This example script demonstrates how to add Expectations to an
 Expectation Suite.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""

# <snippet name="core/expectation_suites/_examples/add_expectations_to_an_expectation_suite.py full example code">
import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core.expectation_suite import ExpectationSuite

context = gx.get_context()
suite = context.suites.add(ExpectationSuite(name="my_expectation_suite"))

expectation = gxe.ExpectColumnValuesToBeInSet(
    column="passenger_count", value_set=[1, 2, 3, 4, 5]
)

# highlight-start
# <snippet name="core/expectation_suites/_examples/add_expectations_to_an_expectation_suite.py add an Expectation to an Expectation Suite">
suite.add_expectation(expectation)
# </snippet>
# highlight-end

# highlight-start
# <snippet name="core/expectation_suites/_examples/add_expectations_to_an_expectation_suite.py create and add an Expectation">
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="pickup_datetime"))
# </snippet>
# highlight-end
# </snippet>
