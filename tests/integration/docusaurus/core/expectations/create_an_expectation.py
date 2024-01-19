"""
This example script demonstrates how to add Expectations to an
 Expectation Suite.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""
# <snippet name="tests/integration/docusaurus/core/expectations/create_an_expectation.py full example code">
# 1. Import the GX Core library's `expectations` module.
# <snippet name="tests/integration/docusaurus/core/expectations/create_an_expectation.py imports">
import great_expectations.expectations as gxe

# </snippet>
# 2. Initialize an Expectation
# <snippet name="tests/integration/docusaurus/core/expectations/create_an_expectation.py initialize Expectations">
expectation = gxe.ExpectColumnValuesToBeInSet(
    column="passenger_count", value_set=[1, 2, 3, 4, 5]
)
# </snippet>
# </snippet>
