"""
This example script demonstrates how to edit a single Expectation in an
 Expectation Suite.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""
# <snippet name="tests/integration/docusaurus/core/expectations/modify_an_expectation_suite.py full example code">
# The following sections set up an Expectation Suite that can later be modified
# as an example.
import great_expectations as gx

# This section creates a new Expectation Suite, which will later have Expectations
# added to it.
# Disregard this code if you are retrieving an existing Expectation Suite.
from great_expectations.core.expectation_suite import ExpectationSuite

new_suite_name = "my_expectation_suite"
new_suite = ExpectationSuite(name=new_suite_name)

context = gx.get_context()

# This section adds the Expectation Suite created earlier to the Data Context.
# Disregard this line if the Expectation Suite you are retrieving has already
# been added to your Data Context.
suite = context.suites.add(new_suite)

# 1. Get the Expectation Suite to modify.
# If you are editing an existing Expectation Suite, retrieve it with:
existing_suite_name = (
    "my_expectation_suite"  # replace this with the name of your Expectation Suite
)
suite = context.suites.get(name=existing_suite_name)

# 2. Modify the Expectation Suite's attributes.
# <snippet name="tests/integration/docusaurus/core/expectations/modify_an_expectation_suite.py edit attribute">
suite.name = "renamed_expectation_suite"
# </snippet>

# 3. Save the Expectation Suite.
# <snippet name="tests/integration/docusaurus/core/expectations/modify_an_expectation_suite.py save the Expectation">
suite.save()
# </snippet>
# </snippet>
