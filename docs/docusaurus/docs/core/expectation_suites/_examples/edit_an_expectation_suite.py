"""
This example script demonstrates how to edit a single Expectation in an
 Expectation Suite.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""
# <snippet name="core/expectation_suites/_examples/edit_an_expectation_suite.py full example code">
# The following sections set up an Expectation Suite that can later be modified
# as an example.

# <snippet name="core/expectation_suites/_examples/edit_an_expectation_suite.py create expectation suite">
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite

new_suite_name = "my_expectation_suite"
new_suite = ExpectationSuite(name=new_suite_name)

context = gx.get_context()
suite = context.suites.add(new_suite)
# </snippet>

# 1. Get the Expectation Suite to modify.
# If you are editing an existing Expectation Suite, retrieve it with:
existing_suite_name = (
    "my_expectation_suite"  # replace this with the name of your Expectation Suite
)
suite = context.suites.get(name=existing_suite_name)

# 2. Modify the Expectation Suite's attributes.
# <snippet name="core/expectation_suites/_examples/edit_an_expectation_suite.py edit attribute">
suite.name = "renamed_expectation_suite"
# </snippet>

# 3. Save the Expectation Suite.
# <snippet name="core/expectation_suites/_examples/edit_an_expectation_suite.py save the Expectation">
suite.save()
# </snippet>
# </snippet>
