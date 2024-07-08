"""
This example script demonstrates how to delete an existing Expectation Suite
from a Data Context.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""

# <snippet name="core/expectation_suites/_examples/delete_an_expectation_suite.py full example code">
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite

context = gx.get_context()

new_suite_name = "my_deletable_expectation_suite"
new_suite = ExpectationSuite(name=new_suite_name)
context.suites.add(new_suite)


# highlight-start
# <snippet name="core/expectation_suites/_examples/delete_an_expectation_suite.py get Expectation Suite">
suite_name = "my_deletable_expectation_suite"
suite = context.suites.get(suite_name)
# </snippet>
# highlight-end

# highlight-start
# <snippet name="core/expectation_suites/_examples/delete_an_expectation_suite.py delete Expectation Suite">
context.suites.delete(name=suite_name)
# </snippet>
# highlight-end
# </snippet>
