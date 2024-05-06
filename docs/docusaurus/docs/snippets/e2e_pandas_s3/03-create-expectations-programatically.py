import great_expectations.expectations as gxe
from great_expectations import get_context

# TODO: will become from great_expectations import get_context, ExpectationSuite
from great_expectations.core import ExpectationSuite
from great_expectations.exceptions import DataContextError

context = get_context(project_root_dir="./")

# Create Expectation Suite
try:
    suite = context.suites.get("project_name")
# TODO: instead of DataContextError will be ResourceNotFoundError
except DataContextError:
    suite = context.suites.add(ExpectationSuite(name="project_name"))

suite.expectations = [
    gxe.ExpectColumnToExist(column="VendorID", column_index=None),
    gxe.ExpectColumnDistinctValuesToBeInSet(
        column="VendorID", value_set=[1, 2, 3, 4, 5, 6]
    ),
    gxe.ExpectColumnValuesToBeBetween(
        column="passenger_count", min_value=1, max_value=5, mostly=0.95
    ),
    gxe.ExpectColumnValuesToBeBetween(
        column="passenger_count", min_value=0, max_value=10
    ),
]

suite.save()
