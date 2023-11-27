# <snippet name="tutorials/quickstart/quickstart.py import_gx">
import great_expectations as gx
from great_expectations import ExpectColumnValuesToNotBeNull, ExpectColumnValuesToBeBetween
from great_expectations.splitters import YearMonthDaySplitter, RangeSplitter
# </snippet>

# Set up
# <snippet name="tutorials/quickstart/quickstart.py get_context">
context = gx.get_context()
# </snippet>

# Connect to data
# <snippet name="tutorials/quickstart/quickstart.py connect_to_data">
datasource = context.sources.add_postresql(name="warehouse", connection_string="postgresql://localhost")
asset = datasource.add_table_asset(table_name="taxi")
bc = asset.add_batch_config(name="daily", splitters=[YearMonthDaySplitter(column="pu_datetime")])
# If you get multiple batches here, you could just use list comprehension. BUT, then
# you have to deal with multiple batches.
# If you want one batch, you can set up a different batch config
bc_range = asset.add_batch_config(name="range", splitters=[RangeSplitter(column="pu_datetime")])
batch = bc_range.get_batch(options={min_date: "2023-01-20", max_date: "2023-01-29"})
batch = bc.get_batch(options={date="2023-01-23"})
# </snippet>

# Create Expectations
# <snippet name="tutorials/quickstart/quickstart.py create_expectation">
suite = context.add_expectation_suite("taxi")
expectation = ExpectColumnValuesToNotBeNull("pickup_datetime")
batch.validate(expectation)
suite.add_expectation(expectation)
suite.add_expectation(
    ExpectColumnValuesToBeBetween("passenger_count", min_value=1, max_value=6)
)
# </snippet>

# Add Conditional Expectations
# <snippet name="tutorials/quickstart/quickstart.py add_conditional_expectation">
r1_suite = context.add_expectation_suite("quickstart.ratecode_1")
r1 = asset.add_batch_config(name="ratecode_1", splitters=[YearMonthDaySplitter(column="pu_datetime"), ColumnValueSplitter("ratecodeid", value=1)])
suite.add_expectation(
    gx.ExpectColumnValuesToBeInSet(column="ratecodeid", value_set=[1])  # This is a "dumb" expectation, but I just want to know my splitter worked
)
suite.add_expectation(
    gx.ExpectColumnMinToBeBetween(column="passenger_count", min_value=2, note="There should be at least 2 passengers in a ratecode 1 trip")
)
batch = r1.get_batch(date="2023-01-23")
batch.validate(suite)  # TODO: confirm we can use `validate` with both expectation and suite (or is this validate_suite?)


# Validate data
# <snippet name="tutorials/quickstart/quickstart.py create_checkpoint">
batch_expectations = context.batch_expectations.add_group(
    name="taxi"
)
batch_expectations.add(
    name="daily",
    expectation_suite=suite,
    # Oops! I lost my reference and need to get it by name
    batch_config=context.sources.postgresql.get_asset("taxi").get_batch_config("daily")
)
batch_expectations.add(
    name="r1"
    expectation_suite=r1_suite,
    batch_config=r1
)
# TODO: add gets by id in this workflow
# </snippet>

# <snippet name="tutorials/quickstart/quickstart.py run_checkpoint">
results = batch_expectations.run()
# </snippet>

# View results
# <snippet name="tutorials/quickstart/quickstart.py view_results">
context.view_validation_result(results)
# </snippet>
