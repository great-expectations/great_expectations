"""
This example script demonstrates how to create a new Expectation Suite.
The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""
# 1. Import the GX Core library and the `ExpectationSuite` class.
# <snippet name="tests/integration/docusaurus/core/expectations/create_an_expectation_suite.py full example code">
# <snippet name="tests/integration/docusaurus/core/expectations/create_an_expectation_suite.py imports">
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite

# </snippet>

# 2. Get a Data Context
# <snippet name="tests/integration/docusaurus/core/expectations/create_an_expectation_suite.py get_context">
context = gx.get_context()
# </snippet>

# 3. Create an Expectation Suite
# <snippet name="tests/integration/docusaurus/core/expectations/create_an_expectation_suite.py create Expectation Suite">
new_suite_name = "my_first_expectation_suite"
suite = ExpectationSuite(name=new_suite_name)
# </snippet>

# 4. Add the Expectation Suite to the Data Context
# <snippet name="tests/integration/docusaurus/core/expectations/create_an_expectation_suite.py add snippet to Data Context">
suite = context.suites.add(suite)
# </snippet>

# Tip: Create an Expectation Suite and add it to the Data Context at the same time
# <snippet name="tests/integration/docusaurus/core/expectations/create_an_expectation_suite.py create and add Expectation Suite to Data Context">
new_suite_name = "my_second_expectation_suite"
suite = context.suites.add(ExpectationSuite(name=new_suite_name))
# </snippet>
# </snippet>
