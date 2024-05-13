"""
This example script allows the user to try out GX by validating Expectations
 against provided sample data.

The snippet tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""

# <snippet name="docs/docusaurus/docs/core/introduction/try_gx.py full example script">
# Import required modules from the GX library
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx.py imports">
import great_expectations as gx
import great_expectations.expectations as gxe

# </snippet>

# Create a temporary Data Context and connect to provided sample data.
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx.py set up">
context = gx.get_context()
batch = context.data_sources.pandas_default.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)
# </snippet>

# Create an Expectation
# highlight-start
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx.py create an expectation">
expectation = gxe.ExpectColumnValuesToBeBetween(
    column="passenger_count", min_value=1, max_value=6
)
# </snippet>
# highlight-end

# Validate the sample data against your Expectation and view the results
# highlight-start
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx.py validate and view results">
validation_result = batch.validate(expectation)
print(validation_result.describe())
# </snippet>
# highlight-end
# </snippet>

output1 = """
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx.py output1">
{
    "expectation_type": "expect_column_values_to_be_between",
    "success": false,
    "kwargs": {
        "batch_id": "default_pandas_datasource-#ephemeral_pandas_asset",
        "column": "passenger_count",
        "min_value": 1.0,
        "max_value": 3.0
    },
    "result": {
        "element_count": 10000,
        "unexpected_count": 853,
        "unexpected_percent": 8.53,
        "partial_unexpected_list": [
            4,
            4,
            4,
            4,
            4,
            4,
            4,
            4,
            4,
            4,
            4,
            4,
            4,
            4,
            4,
            4,
            4,
            4,
            4,
            4
        ],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 8.53,
        "unexpected_percent_nonmissing": 8.53,
        "partial_unexpected_counts": [
            {
                "value": 4,
                "count": 20
            }
        ],
        "partial_unexpected_index_list": [
            9147,
            9148,
            9149,
            9150,
            9151,
            9152,
            9153,
            9154,
            9155,
            9156,
            9157,
            9158,
            9159,
            9160,
            9161,
            9162,
            9163,
            9164,
            9165,
            9166
        ]
    }
}
# </snippet>
"""

assert validation_result.describe() == output1.strip()
