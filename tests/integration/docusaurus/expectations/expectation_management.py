# setup
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.expectations.core import (
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToNotBeNull,
)

context = gx.get_context()
suite_name = "test-suite"
column_a = "a"
column_b = "b"
column_c = "c"

# create a suite
suite = ExpectationSuite(name=suite_name)
suite = context.suites.add(suite=suite)


# add expectations
expectation_a = ExpectColumnValuesToBeInSet(
    column=column_a, value_set=["foo", "bar", "baz"]
)
suite.add(expectation=expectation_a)
expectation_b = ExpectColumnValuesToNotBeNull(column=column_a)
suite.add(expectation=expectation_b)

# find and update a single expectation
expectation_type = ExpectColumnValuesToNotBeNull
expectation = next(
    expectation
    for expectation in suite.expectations
    if isinstance(expectation, expectation_type)
)
expectation.column = column_b
expectation.save()

# batch update expectations
for expectation in suite.expectations:
    expectation.column = column_c
suite.save()

# delete an expectation
expectation_type = ExpectColumnValuesToNotBeNull
expectation = next(
    expectation
    for expectation in suite.expectations
    if isinstance(expectation, expectation_type)
)
suite.delete(expectation=expectation)

# get a suite
suite = context.suites.get(name=suite_name)

# delete a suite
context.suites.delete(suite=suite)
