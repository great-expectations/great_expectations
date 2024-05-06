# TODO: will become from great_expectations import get_context, ExpectationSuite
import great_expectations.expectations as gxe
from great_expectations import get_context
from great_expectations.core import ExpectationSuite
from great_expectations.exceptions import DataContextError

context = get_context(project_root_dir="./")

try:
    suite = context.suites.get("project_name")
# TODO: error will change to ResourceNotFoundError
except DataContextError:
    # TODO: will change to:
    # suite = context.suites.add(name="project_name")
    suite = context.suites.add(ExpectationSuite(name="project_name"))

batch = context.data_sources.pandas_default.read_parquet(
    "s3://nyc-tlc/trip data/yellow_tripdata_2019-01.parquet"
)

# TODO: column_index will not be required
expectation = gxe.ExpectColumnToExist(column="VendorID", column_index=None)
result = batch.validate(expectation)
print(result)
suite.add_expectation(expectation)

expectation = gxe.ExpectColumnValuesToMatchRegex(column="VendorID", regex="^[123456]$")
result = batch.validate(expectation)
print(result)
suite.add_expectation(expectation)

expectation = gxe.ExpectColumnValuesToBeUnique(column="VendorID")
result = batch.validate(expectation)
print(result)
suite.add_expectation(expectation)

print(suite)
