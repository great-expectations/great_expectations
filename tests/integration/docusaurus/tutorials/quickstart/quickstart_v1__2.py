# <snippet name="tutorials/quickstart/quickstart.py import_gx">
import great_expectations as gx

# </snippet>

# Set up
# <snippet name="tutorials/quickstart/quickstart.py get_context">
context = gx.get_context()
# </snippet>

# Connect to data
# <snippet name="tutorials/quickstart/quickstart.py connect_to_data">
validator = context.sources.pandas_default.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)
# </snippet>

# Update Expectations
# <snippet name="tutorials/quickstart/quickstart.py update_expectation">
suite = context.get_expectation_suite("quickstart")
# Note how e find expectations now. It's a list filtering operation left to the user.
expectation = [expectation in suite.expectations if expectation.expectation_type == "expect_column_values_to_not_be_null" and expectation.column == "pickup_datetime"][0]
expectation = [expectation in suite.expectations if isinstance(expectation, ExpectColumnValuesToNotBeNull) and expectation.column == "pickup_datetime"][0]  # Expectations are of the Expectation Type
expectation.mostly = 0.9
suite.update(expectation)
# </snippet>


expectation = ExpectColumnValuesToBeInSet(column="id", value_set=[1,2,3])
validator.validate(expectation)

# Note that this assumes we've got a true set, but that we coerced from a list (@chetan)
expectation.value_set.add(4)
validator.validate(expectation)
suite.add_expectation(
    expectation
)
