"""
This example script allows the user to try out GX by validating Expectations
 against provided sample data.

The <snippet> tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""

# <snippet name="docs/docusaurus/docs/core/introduction/try_gx.py full example script">
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx.py imports">
import great_expectations as gx
import great_expectations.expectations as gxe

# </snippet>

# <snippet name="docs/docusaurus/docs/core/introduction/try_gx.py set up">
context = gx.get_context()
batch = context.sources.pandas_default.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)
# </snippet>

# <snippet name="docs/docusaurus/docs/core/introduction/try_gx.py create an expectation>
expectation = gxe.ExpectColumnValuesToBeBetween(
    column="passenger_count", min_value=1, max_value=6
)
# </snippet>

# <snippet name="docs/docusaurus/docs/core/introduction/try_gx.py validate and view results>
validation_result = batch.validate(expectation)
print(validation_result.describe())
# </snippet>
# </snippet>

# <snippet name="docs/docusaurus/docs/core/introduction/try_gx.py try another expectation 1>
expectation = gxe.ExpectColumnValuesToBeBetween(
    column="passenger_count", min_value=1, max_value=6
)
validation_result = batch.validate(expectation)
print(validation_result.describe())
# </snippet>

# <snippet name="docs/docusaurus/docs/core/introduction/try_gx.py try another expectation 2>
expectation = gxe.ExpectColumnValuesToBeBetween(
    column="passenger_count", min_value=1, max_value=6
)
validation_result = batch.validate(expectation)
print(validation_result.describe())
# </snippet>

# <snippet name="docs/docusaurus/docs/core/introduction/try_gx.py try another expectation 3>
expectation = gxe.ExpectColumnValuesToBeBetween(
    column="passenger_count", min_value=1, max_value=6
)
validation_result = batch.validate(expectation)
print(validation_result.describe())
# </snippet>
