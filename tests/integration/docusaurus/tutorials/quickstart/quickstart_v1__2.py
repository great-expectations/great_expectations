# <snippet name="tutorials/quickstart/quickstart.py import_gx">
import great_expectations as gx

# </snippet>

# Set up
# <snippet name="tutorials/quickstart/quickstart.py get_context">
context = gx.get_context()
# </snippet>

# Connect to data
# <snippet name="tutorials/quickstart/quickstart.py connect_to_data">
batch = context.sources.pandas_default.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)
# </snippet>

# Update Expectations
# <snippet name="tutorials/quickstart/quickstart.py update_expectation">
suite = context.get_expectation_suite("quickstart")
# Note how we find expectations now. It's a list filtering operation left to the user.
expectation = [expectation in suite.expectations if expectation.expectation_type == "expect_column_values_to_not_be_null" and expectation.column == "pickup_datetime"][0]
expectation = [expectation in suite.expectations if isinstance(expectation, ExpectColumnValuesToNotBeNull) and expectation.column == "pickup_datetime"][0]  # Expectations are of the Expectation Type
expectation.mostly = 0.9
# suite.update(expectation)  # TODO: resolve persistence question. I think this should become unnecessary
# </snippet>


expectation = gx.ExpectColumnValuesToBeInSet(column="id", value_set={1,2,3})
expectation = gx.ExpectColumnValuesToBeInSet(column="id", value_set=[1,2,3])  # Note we allow both types here with pydantic validation and coercion
batch.validate(expectation)

# Demo beat: notice we can use natural python operations on parameters now!
expectation.value_set.add(4)
batch.validate(expectation)
suite.add_expectation(
    expectation
)
