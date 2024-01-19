"""
This example script demonstrates how to add Expectations to an
 Expectation Suite.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""
# <snippet name="tests/integration/docusaurus/core/expectation_suite/add_expectations_to_an_expectation_suite.py full example code">
# 1. Import the GX Core library and `expectations` module.
# <snippet name="tests/integration/docusaurus/core/expectation_suite/add_expectations_to_an_expectation_suite.py imports">
import great_expectations as gx
import great_expectations.expectations as gxe

# </snippet>
# For this example to run as a stand alone script, we need to create
# the Expectation Suite that will later be retrieved and have Expectations
# added to it.  This requires the import of the `ExpectationSuite` class.
from great_expectations.core.expectation_suite import ExpectationSuite

# 2. Get a Data Context
# <snippet name="tests/integration/docusaurus/core/expectation_suite/add_expectations_to_an_expectation_suite.py get_context">
context = gx.get_context()
# </snippet>

# 3. Create a new or retrieve an existing Expectation Suite
# This section adds a new Expectation Suite to the Data Context.
#  Disregard this line if the Expectation Suite you are retrieving has already
#  been added to your Data Context.
context.suites.add(ExpectationSuite(name="my_empty_expectation_suite"))

# To retrieve an existing suite, use the following:
# <snippet name="tests/integration/docusaurus/core/expectation_suite/add_expectations_to_an_expectation_suite.py get_suite">
existing_suite_name = (
    "my_empty_expectation_suite"  # replace this with the name of your Expectation Suite
)
suite = context.suites.get(name=existing_suite_name)
# </snippet>

# 4. Create an Expectation
# <snippet name="tests/integration/docusaurus/core/expectation_suite/add_expectations_to_an_expectation_suite.py create an Expectation">
expectation = gxe.ExpectColumnValuesToBeInSet(
    column="passenger_count", value_set=[1, 2, 3, 4, 5]
)
# </snippet>

# 5. Add the Expectation to the Expectation Suite
# <snippet name="tests/integration/docusaurus/core/expectation_suite/add_expectations_to_an_expectation_suite.py add an Expectation to an Expectation Suite">
suite.add_expectation(expectation)
# </snippet>

# Tip: You can create an Expectation at the same time as you add it to an Expectation suite with:
# <snippet name="tests/integration/docusaurus/core/expectation_suite/add_expectations_to_an_expectation_suite.py create and add an Expectation">
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="pickup_datetime"))
# </snippet>
# </snippet>
