"""
This example script demonstrates how to create an Expectation outside
  of an Expectation Suite.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""

# <snippet name="tests/integration/docusaurus/core/expectations/create_an_expectation.py full example code">
# <snippet name="core/create_expectations/expectations/_examples/create_an_expectation.py import the expectations module">
import great_expectations.expectations as gxe

# </snippet>

# highlight-start
# <snippet name="core/create_expectations/expectations/_examples/create_an_expectation.py create the expectation">
expectation = gxe.ExpectColumnValuesToBeInSet(
    column="passenger_count", value_set=[1, 2, 3, 4, 5]
)
# </snippet>
# highlight-end
# </snippet>
