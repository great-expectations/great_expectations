# setup
import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core.expectation_suite import ExpectationSuite

context = gx.get_context()
suite_name = "test-suite"

# create a suite
suite = context.suites.add(suite=ExpectationSuite(name=suite_name))

# get a suite
suite = context.suites.get(name=suite_name)

# add expectations
suite.add_expectation(
    gxe.ExpectColumnValuesToBeInSet(column="passenger_count", value_set=[1, 2, 3, 4, 5])
)
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="pickup_datetime"))

# find a single expectation by type and/or domain
expectation = next(
    expectation
    for expectation in suite.expectations
    if isinstance(expectation, gxe.ExpectColumnValuesToNotBeNull)
    and expectation.column == "pickup_datetime"
)

# update a single expectation
expectation = next(
    expectation
    for expectation in suite.expectations
    if isinstance(expectation, gxe.ExpectColumnValuesToNotBeNull)
)
expectation.column = "pickup_location_id"
expectation.save()

# update multiple expectations as a batch
for expectation in suite.expectations:
    expectation.notes = "This Expectation was generated as part of GX Documentation."
suite.save()

# delete an expectation
expectation = next(
    expectation
    for expectation in suite.expectations
    if isinstance(expectation, gxe.ExpectColumnValuesToNotBeNull)
)
suite.delete_expectation(expectation=expectation)

# delete a suite
context.suites.delete(name=suite_name)
