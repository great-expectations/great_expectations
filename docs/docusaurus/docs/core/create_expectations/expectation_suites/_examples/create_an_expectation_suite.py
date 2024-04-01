"""
This example script demonstrates how to create a new Expectation Suite.
The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""

# <snippet name="core/expectation_suites/_examples/create_an_expectation_suite.py full example code">
# <snippet name="core/expectation_suites/_examples/create_an_expectation_suite.py imports">
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite

# </snippet>

context = gx.get_context()

# highlight-start
# <snippet name="core/expectation_suites/_examples/create_an_expectation_suite.py create Expectation Suite">
new_suite_name = "my_first_expectation_suite"
suite = ExpectationSuite(name=new_suite_name)
# </snippet>
# highlight-end

# highlight-start
# <snippet name="core/expectation_suites/_examples/create_an_expectation_suite.py add snippet to Data Context">
suite = context.suites.add(suite)
# </snippet>
# highlight-end

# highlight-start
# <snippet name="core/expectation_suites/_examples/create_an_expectation_suite.py create and add Expectation Suite to Data Context">
new_suite_name = "my_second_expectation_suite"
suite = context.suites.add(ExpectationSuite(name=new_suite_name))
# </snippet>
# highlight-end
# </snippet>
