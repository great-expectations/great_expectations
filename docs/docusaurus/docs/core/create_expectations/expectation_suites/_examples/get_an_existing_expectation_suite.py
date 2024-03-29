"""
This example script demonstrates how to get an existing Expectation Suite
 from a Data Context.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""

# <snippet name="core/expectation_suites/_examples/get_an_expectation_suite.py full example code">
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite

context = gx.get_context()
new_suite_name = "my_expectation_suite"
context.suites.add(ExpectationSuite(name=new_suite_name))

# highlight-start
# <snippet name="core/expectation_suites/_examples/get_an_expectation_suite.py create Expectation Suite">
existing_suite_name = (
    "my_expectation_suite"  # replace this with the name of your Expectation Suite
)
suite = context.suites.get(name=existing_suite_name)
# </snippet>
# highlight-end
# </snippet>
