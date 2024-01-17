"""
This example script demonstrates how to get an existing Expectation Suite
 from a Data Context.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""
# <snippet name="tests/integration/docusaurus/core/expectations/get_an_expectation_suite.py full example code">
# 1. Import the Great Expectations library
# <snippet name="tests/integration/docusaurus/core/expectations/get_an_expectation_suite.py imports">
import great_expectations as gx

# </snippet>
# This section creates the Expectation Suite that will later be retrieved.
#   Disregard these lines if you are retrieving an Expectation Suite that has
#   already been created.
from great_expectations.core.expectation_suite import ExpectationSuite

new_suite_name = "my_first_expectation_suite"
new_suite = ExpectationSuite(name=new_suite_name)

# 2. Get a Data Context
# <snippet name="tests/integration/docusaurus/core/expectations/get_an_expectation_suite.py get_context">
context = gx.get_context()
# </snippet>

# This section adds the Expectation Suite created earlier to the Data Context.
#  Disregard this line if the Expectation Suite you are retrieving has already
#  been added to your Data Context.
suite = context.suites.add(new_suite)

# 3. Use the Data Context to retrieve the existing Expectation Suite.
# <snippet name="tests/integration/docusaurus/core/expectations/get_an_expectation_suite.py create Expectation Suite">
existing_suite_name = (
    "my_first_expectation_suite"  # replace this with the name of your Expectation Suite
)
suite = context.suites.get(name=existing_suite_name)
# </snippet>
# </snippet>
