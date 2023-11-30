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
suite = context.add_expectation_suite("quickstart")
expectation = gx.ExpectColumnValuesToNotBeNull("pickup_datetime")
validator.validate(expectation)
suite.add_expectation(expectation)
suite.add_expectation(
    ExpectColumnValuesToBeBetween("passenger_count", min_value=1, max_value=6)  # Note: we are removing "auto" at this point
)
# </snippet>

# Validate data
# <snippet name="tutorials/quickstart/quickstart.py create_checkpoint">
batch_expectations = context.add_batch_expectations(
    name="quickstart",
    expectation_suite=suite,
    batch_config=validator.batch_config,    # not discussed yet
)
# </snippet>

# <snippet name="tutorials/quickstart/quickstart.py run_checkpoint">
results = batch_expectations.run()
# </snippet>

# View results
# <snippet name="tutorials/quickstart/quickstart.py view_results">
context.view_validation_result(results)
# </snippet>



suite = context.add_expectation_suite("foo.taxi")

# Notice that we're now breaking the creation and testing of the expectation into multiple steps
# each of which is more explicit than before
expectation = ExpectColumnValuesToBeInSet(column="id", value_set=[1,2,3])
validator.validate(expectation)

# Note that this assumes we've got a true set, but that we coerced from a list (@chetan)
expectation.value_set.add(4)
validator.validate(expectation)
suite.add_expectation(
    expectation
)

# Note! This makes BatchExpectations require a name and be attached to a data context
be = context.batch_expectations.add(name="taxi", batch_config=bc, expectation_suite=suite)
be.validate()  # picks up the default values for options

checkpoint = context.add_checkpoint(name="taxi", batch_expectations=be, action_list=[EmailAction()])
