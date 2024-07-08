"""
This example script demonstrates how to edit an Expectation.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""

# <snippet name="core/expectations/_examples/edit_an_expectation.py full example code">
import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core.expectation_suite import ExpectationSuite

context = gx.get_context()
suite = context.suites.add(ExpectationSuite(name="my_expectation_suite"))
expectation = suite.add_expectation(
    gxe.ExpectColumnValuesToBeInSet(column="passenger_count", value_set=[1, 2])
)

# highlight-start
# <snippet name="core/create_expectations/expectations/_examples/edit_an_expectation.py modify the expectation">
expectation.value_set = [1, 2, 3, 4, 5]
# </snippet>
# <snippet name="core/create_expectations/expectations/_examples/edit_an_expectation.py save the expectation">
expectation.save()
# </snippet>
# highlight-end
# </snippet>
