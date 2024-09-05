# <snippet name="docs/docusaurus/docs/snippets/unexpected_row_expectation.py imports">
import great_expectations as gx

# These imports need to be cleaned up
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.expectations import UnexpectedRowsExpectation

# </snippet>

# <snippet name="docs/docusaurus/docs/snippets/unexpected_row_expectation.py set_up_context">
context = gx.get_context()
# </snippet>

# This utility is not for general use. It is only to support testing.
from tests.test_utils import load_data_into_test_database

# The following load & config blocks up until the batch requests are only to support testing.
PG_CONNECTION_STRING = "postgresql+psycopg2://postgres:@localhost/test_ci"

load_data_into_test_database(
    table_name="postgres_taxi_data",
    csv_path="./data/yellow_tripdata_sample_2019-01.csv",
    connection_string=PG_CONNECTION_STRING,
    convert_colnames_to_datetime=["pickup_datetime", "pickup_datetime"],
)


# <snippet name="docs/docusaurus/docs/snippets/unexpected_row_expectation.py define_custom_expectation">
class UnexpectedTripDistance(UnexpectedRowsExpectation):
    unexpected_rows_query: str = """
        SELECT
            vendor_id, pickup_datetime
        FROM
            postgres_taxi_data
        WHERE
            trip_distance > 20
    """
    description = "Trips should be less than 20 miles"


# </snippet>

# <snippet name="docs/docusaurus/docs/snippets/unexpected_row_expectation.py define_batch_definition">
batch_definition = (
    context.data_sources.add_postgres(
        name="pg_datasource", connection_string=PG_CONNECTION_STRING
    )
    .add_table_asset(name="postgres_taxi_data", table_name="postgres_taxi_data")
    .add_batch_definition_daily(name="daily", column="pickup_datetime")
)
# </snippet>

# <snippet name="docs/docusaurus/docs/snippets/unexpected_row_expectation.py define_expectation_suite">
expectation = UnexpectedTripDistance()
suite = context.suites.add(ExpectationSuite("my_suite", expectations=[expectation]))
# </snippet>

# <snippet name="docs/docusaurus/docs/snippets/unexpected_row_expectation.py validate_suite">
validation_definition = ValidationDefinition(
    name="my_validation", data=batch_definition, suite=suite
)
result = validation_definition.run()
# </snippet>

assert not result.success
assert len(result.results) == 1
