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

# Note how we find expectations now. It's a list filtering operation left to the user.

# Update Expectations
# <snippet name="tutorials/quickstart/quickstart.py update_expectation">
suite = context.get_expectation_suite("quickstart")
expectation = [
    expectation
    for expectation in suite.expectations
    if isinstance(expectation, gxe.ExpectColumnValuesToNotBeNull)
    and expectation.column == "pickup_datetime"
][0]
expectation.mostly = 0.9
expectation.save()
# </snippet>

# Note we allow both list and set here, thanks to pydantic validation and coercion
expectation = gxe.ExpectColumnValuesToBeInSet(column="id", value_set={1, 2, 3})

# <snippet name="tutorials/quickstart/quickstart.py update_expectation_using_typed_parameters">
expectation = gxe.ExpectColumnValuesToBeInSet(column="id", value_set=[1, 2, 3])
expectation.value_set.add(4)
expectation.save()
# </snippet>
