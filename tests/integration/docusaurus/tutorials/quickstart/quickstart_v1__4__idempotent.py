"""
This example shows how to configure an asset, create expectations, and set up data validation
in code that is suitable for use in a CI system.

It focuses on providing an example of how to tackle state management in a production
environment, ensuring that you have a python-based source of truth for configuration.

Key beats:

1. addressing idempotency
2. when to use name vs. id
"""
# <snippet name="tutorials/quickstart/quickstart.py import_gx">
from great_expectations import get_context, ExpectationSuite
from great_expectations.expectations import (
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToNotBeNull,
)
from great_expectations.datasources import YearMonthDaySplitter
# </snippet>

# Set up
# <snippet name="tutorials/quickstart/quickstart.py get_context">
context = get_context()
# </snippet>

# Connect to data
# <snippet name="tutorials/quickstart/quickstart.py connect_to_data">
# we use get by name here instead of id...

# TRY / EXCEPT
try:
    datasource = context.sources.get_by_name("warehouse")
except KeyError:
    datasource = context.sources.add_postresql(
        name="warehouse", connection_string="postgresql://localhost"
    )

try:
    asset = datasource.get_table_asset("taxi")
except KeyError:
    asset = datasource.add_table_asset(asset_name="taxi", table_name="taxi")

try:
    daily = asset.get_batch_config("daily")
except KeyError:
    daily = asset.add_batch_config(
        name="daily", splitters=[YearMonthDaySplitter(column="pu_datetime")]
    )
# </snippet>

# Create Expectations
# <snippet name="tutorials/quickstart/quickstart.py create_expectation">
suite = ExpectationSuite(name="taxi")
suite.add(ExpectColumnValuesToNotBeNull("pickup_datetime"))
suite.add(ExpectColumnValuesToBeBetween("passenger_count", min_value=1, max_value=6))
# </snippet>


# Set up Validation
# <snippet name="tutorials/quickstart/quickstart.py create_checkpoint">
validation = context.validations.add(
    name="daily",
    expectation_suite=suite,
    batch_config=daily,
)

checkpoint = context.checkpoints.add(
    name="taxi", validations=[validation], action_list=[SendSlackNotificationAction()]
)

# TODO: why both a validation and a checkpoint here??
# </snippet>

# <snippet name="tutorials/quickstart/quickstart.py run_checkpoint">
# TODO: get_checkpoint or .checkpoints
results = context.get_checkpoint(name="taxi").run()
results = context.get_checkpoint(id="ugly-uuid").run()
# </snippet>
