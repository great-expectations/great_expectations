"""
This example shows how to configure an asset, create expectations, set up data validation.

It serves as an example for how a user is likely to build expectations for a production environment.
1. split this into two
2. add state management example (add/update)
"""
# <snippet name="tutorials/quickstart/quickstart.py import_gx">
import great_expectations as gx
from great_expectations.expectations import (
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeBetween,
)
# TODO: @tyler Ticket for splitters, including the correct namespace and validating the list of ones that will work
from great_expectations.splitters import YearMonthDaySplitter, RangeSplitter
# </snippet>

# Set up
# <snippet name="tutorials/quickstart/quickstart.py get_context">
context = gx.get_context()
# </snippet>

# Connect to data
# <snippet name="tutorials/quickstart/quickstart.py connect_to_data">
# TODO: idempotency + ids?
datasource = context.sources.add_postresql(
    name="warehouse", connection_string="postgresql://localhost"
)
asset = datasource.add_table_asset(table_name="taxi")
daily = asset.add_batch_config(
    name="daily", splitters=[YearMonthDaySplitter(column="pu_datetime")]
)
# </snippet>

# Create Expectations
# <snippet name="tutorials/quickstart/quickstart.py create_expectation">
suite = context.add_expectation_suite("taxi")
suite.add(ExpectColumnValuesToNotBeNull("pickup_datetime"))
suite.add(
    ExpectColumnValuesToBeBetween("passenger_count", min_value=1, max_value=6)
)
# </snippet>


# Set up Validation
# <snippet name="tutorials/quickstart/quickstart.py create_checkpoint">
validation = context.validations.add(
    name="daily",
    expectation_suite=suite,
    batch_config=daily,
)

checkpoint = context.checkpoints.add(
    name="taxi", validations = [validation], action_list=[SendSlackNotificationAction()]
)

# TODO: why both a validation and a checkpoint here??
# </snippet>

# <snippet name="tutorials/quickstart/quickstart.py run_checkpoint">
# TODO: get_checkpoint or .checkpoints
results = context.get_checkpoint("taxi").run()
# </snippet>

# View results
# <snippet name="tutorials/quickstart/quickstart.py view_results">
results.open()
# </snippet>
