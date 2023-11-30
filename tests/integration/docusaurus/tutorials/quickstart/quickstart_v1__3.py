"""
This example shows how to configure an assets for a variety of scenarios in Great Expectations.
"""

# <snippet name="tutorials/quickstart/quickstart.py import_gx">
import great_expectations as gx
from great_expectations.splitters import RangeSplitter, YearMonthDaySplitter

# </snippet>

# Set up
# <snippet name="tutorials/quickstart/quickstart.py get_context">
context = gx.get_context()
# </snippet>

# Connect to data
# <snippet name="tutorials/quickstart/quickstart.py connect_to_data">
datasource = context.sources.add_postresql(
    name="warehouse", connection_string="postgresql://localhost"
)
asset = datasource.add_table_asset(table_name="taxi")
all = asset.add_batch_config(name="all", splitters=[])
daily = asset.add_batch_config(
    name="daily", splitters=[YearMonthDaySplitter(column="pu_datetime")]
)
range = asset.add_batch_config(
    name="range", splitters=[RangeSplitter(column="pu_datetime")]
)
batch = range.get_batch(options={"min_date": "2023-01-20", "max_date": "2023-01-29"})
batch = daily.get_batch(options={"date": "2023-01-23"})
# </snippet>
