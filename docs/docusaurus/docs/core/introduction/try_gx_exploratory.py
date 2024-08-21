"""
This example script allows the user to try out GX by validating Expectations
 against sample data.

The snippet tags are used to insert the corresponding code into the
 Great Expectations documentation.  They can be disregarded by anyone
 reviewing this script.
"""
# ruff: noqa: I001
# Adding noqa rule so that GX and Pandas imports don't get reordered by linter.

# <snippet name="docs/docusaurus/docs/core/introduction/try_gx_exploratory.py full exploratory script">

# Import required modules from GX library.
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx_exploratory.py import gx library">
import great_expectations as gx

import pandas as pd
# </snippet>

# Create Data Context.
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx_exploratory.py create data context">
context = gx.get_context()
# </snippet>

# Import sample data into Pandas DataFrame.
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx_exploratory.py import sample data">
df = pd.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)
# </snippet>

# Connect to data.
# Create Data Source, Data Asset, Batch Definition, and Batch.
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx_exploratory.py connect to data and get batch">
data_source = context.data_sources.add_pandas("pandas")
data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")

batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
# </snippet>

# Create Expectation.
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx_exploratory.py create expectation">
expectation = gx.expectations.ExpectColumnValuesToBeBetween(
    column="passenger_count", min_value=1, max_value=6
)
# </snippet>

# Validate Batch using Expectation.
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx_exploratory.py validate batch">
validation_result = batch.validate(expectation)
# </snippet>

# </snippet>
# Above snippet ends the full exploratory script.

exploratory_output = """
# <snippet name="docs/docusaurus/docs/core/introduction/try_gx_exploratory.py passing output">
{
  "success": true,
  "expectation_config": {
    "type": "expect_column_values_to_be_between",
    "kwargs": {
      "batch_id": "pandas-pd dataframe asset",
      "column": "passenger_count",
      "min_value": 1.0,
      "max_value": 6.0
    },
    "meta": {}
  },
  "result": {
    "element_count": 10000,
    "unexpected_count": 0,
    "unexpected_percent": 0.0,
    "partial_unexpected_list": [],
    "missing_count": 0,
    "missing_percent": 0.0,
    "unexpected_percent_total": 0.0,
    "unexpected_percent_nonmissing": 0.0,
    "partial_unexpected_counts": [],
    "partial_unexpected_index_list": []
  },
  "meta": {},
  "exception_info": {
    "raised_exception": false,
    "exception_traceback": null,
    "exception_message": null
  }
}
# </snippet>
"""

# Test workflow output with passing Expectation.
assert validation_result["success"] is True
assert (
    validation_result["expectation_config"]["type"]
    == "expect_column_values_to_be_between"
)
assert validation_result["result"]["element_count"] == 10_000
