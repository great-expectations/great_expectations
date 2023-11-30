"""
This example shows how to use row conditions to create conditional expectations.
"""
# <snippet name="tutorials/quickstart/quickstart.py import_gx">
import great_expectations as gx
from great_expectations.splitters import YearMonthDaySplitter

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
daily = asset.add_batch_config(
    name="daily", splitters=[YearMonthDaySplitter(column="pu_datetime")]
)
# </snippet>

# Create Expectations
# <snippet name="tutorials/quickstart/quickstart.py create_expectation">
suite = context.add_expectation_suite("taxi")
suite.add(gxe.ExpectColumnValuesToNotBeNull("pickup_datetime"))
suite.add(
    gxe.ExpectColumnValuesToBeBetween("passenger_count", min_value=1, max_value=6)
)
# </snippet>

# Add Conditional Expectations
# <snippet name="tutorials/quickstart/quickstart.py add_conditional_expectation">
# TODO: JPC - I'm back to thinking we want to create this as a batch config *from* the other one
ratecode_1_batch_config = daily.add_batch_config(
    name="ratecode_1",
    splitters=[ColumnValueSplitter("ratecodeid", value=1)],
)
ratecode_1_suite = context.add_expectation_suite("taxi.ratecode_1")
# Older version:
# ratecode_1_batch = asset.add_batch_config(
#     name="ratecode_1",
#     splitters=[
#         YearMonthDaySplitter(column="pu_datetime"),
#         ColumnValueSplitter("ratecodeid", value=1),
#     ],
# )
ratecode_1_suite.add(
    # This is a "dumb" expectation, but I just want to know my splitter worked
    gxe.ExpectColumnValuesToBeInSet(column="ratecodeid", value_set=[1])
)
ratecode_1_suite.add(
    gxe.ExpectColumnMinToBeBetween(
        column="passenger_count",
        min_value=2,
        note="There should be at least 2 passengers in a ratecode 1 trip",
    )
)
batch = ratecode_1_batch_config.get_batch(date="2023-01-23")
# TODO: confirm we can use `validate` with both expectation and suite (or is this validate_suite?)
batch.validate(ratecode_1_suite)


# Validate data
# <snippet name="tutorials/quickstart/quickstart.py create_checkpoint">
# TODO: resolve validation group
validation = context.validations.add_group(name="taxi")
validation.add(
    name="daily",
    expectation_suite=suite,
    # Oops! I lost my reference and need to get it by name
    batch_config=context.sources.postgresql.get_asset("taxi").get_batch_config("daily"),
    actions=[sendslack],
)
validation.add(name="r1", expectation_suite=r1_suite, batch_config=r1)
# </snippet>

# <snippet name="tutorials/quickstart/quickstart.py run_checkpoint">
results = checkpoint.run()
# </snippet>

# View results
# <snippet name="tutorials/quickstart/quickstart.py view_results">
results.open()
# </snippet>
