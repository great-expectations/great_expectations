"""
This example script demonstrates how to delete an existing Expectation Suite
from a Data Context.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""
# <snippet name="core/expectation_suites/_examples/delete_an_expectation_suite.py full example code">
# 1. Import the Great Expectations library
# <snippet name="core/expectation_suites/_examples/delete_an_expectation_suite.py imports">
import great_expectations as gx

# </snippet>
# This section creates the Expectation Suite that will later be deleted.
#   Disregard these lines if you are deleting an Expectation Suite that has
#   already been created.
from great_expectations.core.expectation_suite import ExpectationSuite

new_suite_name = "my_deletable_expectation_suite"
new_suite = ExpectationSuite(name=new_suite_name)

# 2. Get a Data Context
# <snippet name="core/expectation_suites/_examples/delete_an_expectation_suite.py get_context">
context = gx.get_context()
# </snippet>

# This section adds the Expectation Suite created earlier to the Data Context.
#  Disregard this line if the Expectation Suite you are deleting has already
#  been added to your Data Context.
suite = context.suites.add(new_suite)

# 3. Get the Expectation Suite that will be deleted.
# <snippet name="core/expectation_suites/_examples/delete_an_expectation_suite.py get Expectation Suite">
suite_name = "my_deletable_expectation_suite"
suite = context.suites.get(suite_name)
# </snippet>

# 4. Use the Data Context to delete the existing Expectation Suite.
# <snippet name="core/expectation_suites/_examples/delete_an_expectation_suite.py delete Expectation Suite">
context.suites.delete(suite=suite)
# </snippet>
# </snippet>
