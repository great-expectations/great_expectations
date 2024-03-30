"""
This example script demonstrates how to edit all Expectations in an
 Expectation Suite.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""

# <snippet name="core/expectation_suites/_examples/edit_all_expectations_in_an_expectation_suite.py full example code">
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

# highlight-start
# <snippet name="core/expectation_suites/_examples/edit_all_expectations_in_an_expectation_suite.py modify Expectations">
for expectation in suite.expectations:
    expectation.notes = "This Expectation was generated as part of GX Documentation."
# </snippet>
# highlight-end

# highlight-start
# <snippet name="core/expectation_suites/_examples/edit_all_expectations_in_an_expectation_suite.py save Expectation Suite">
suite.save()
# </snippet>
# highlight-end
# </snippet>
