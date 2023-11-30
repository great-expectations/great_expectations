"""
This example shows editing an existing expectation suite.
"""

# <snippet name="tutorials/quickstart/quickstart.py import_gx">
import great_expectations as gx
import great_expectations.expectations as gxe

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
expectation = [
    expectation
    for expectation in suite.expectations
    if expectation.expectation_type == "expect_column_values_to_not_be_null"
    and expectation.column == "pickup_datetime"
][0]
# Expectations are of the Expectation Type
expectation = [
    expectation
    for expectation in suite.expectations
    if isinstance(expectation, gxe.ExpectColumnValuesToNotBeNull)
    and expectation.column == "pickup_datetime"
][0]
expectation.mostly = 0.9
# TODO: ticket to add this to expectation
expectation.save()  # We explicitly save the change
# </snippet>


expectation = gxe.ExpectColumnValuesToBeInSet(column="id", value_set={1, 2, 3})
expectation = gxe.ExpectColumnValuesToBeInSet(
    column="id", value_set=[1, 2, 3]
)  # Note we allow both types here with pydantic validation and coercion
batch.validate(expectation)

# Demo beat: notice we can use natural python operations on parameters now!
# This was a big pain point before!
expectation.value_set.add(4)
batch.validate(expectation)
suite.add(expectation)
