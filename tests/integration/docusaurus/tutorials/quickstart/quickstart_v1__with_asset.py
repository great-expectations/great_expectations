# <snippet name="tutorials/quickstart/quickstart.py import_gx">
import great_expectations as gx
from great_expectations import Validator
from great_expectations.expectations import ExpectColumnValuesToNotBeNull, ExpectColumnValuesToBeBetween

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
# validator = Validator(bc, options={min_date: 1, max_date: 2})  # Options taking a range not discussed yet
validator = Validator(bc, options={date="2023-01-23"})  # Options taking a range not discussed yet
# </snippet>

# Create Expectations
# <snippet name="tutorials/quickstart/quickstart.py create_expectation">
suite = context.add_expectation_suite("taxi")
expectation = ExpectColumnValuesToNotBeNull("pickup_datetime")
validator.validate(expectation)
suite.add_expectation(expectation)
suite.add_expectation(
    ExpectColumnValuesToBeBetween("passenger_count", min_value=1, max_value=6)  # Note: we are removing "auto" at this point
)
# </snippet>

# Validate data
# <snippet name="tutorials/quickstart/quickstart.py create_checkpoint">
# Notice that we're naming batch expectations
batch_expectations = context.add_batch_expectations(
    name="taxi",
    expectation_suite=suite,
    batch_config=bc,
)
# </snippet>

# <snippet name="tutorials/quickstart/quickstart.py run_checkpoint">
results = batch_expectations.run()
# </snippet>

# View results
# <snippet name="tutorials/quickstart/quickstart.py view_results">
context.view_validation_result(results)
# </snippet>
