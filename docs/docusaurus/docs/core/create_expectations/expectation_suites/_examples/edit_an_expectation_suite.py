"""
This example script demonstrates how to edit a single Expectation in an
 Expectation Suite.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""

# <snippet name="core/expectation_suites/_examples/edit_an_expectation_suite.py full example code">
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite

context = gx.get_context()
suite_name = "my_expectation_suite"
suite = context.suites.add(ExpectationSuite(name=suite_name))

# highlight-start
# <snippet name="core/expectation_suites/_examples/edit_an_expectation_suite.py edit attribute">
suite.name = "my_renamed_expectation_suite"
# </snippet>
# highlight-end

# highlight-start
# <snippet name="core/expectation_suites/_examples/edit_an_expectation_suite.py save the Expectation Suite">
suite.save()
# </snippet>
# highlight-end
# </snippet>
