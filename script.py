import great_expectations as gx
from great_expectations.expectations.expectation import SqlExpectation


class ExpectPassengerCountToBeReasonable(SqlExpectation):
    query: str = "SELECT * FROM {active_batch} WHERE passenger_count > 6"


context = gx.get_context(mode="ephemeral")

datasource = context.sources.add_sqlite(
    name="my_sqlite",
    connection_string="sqlite:///yellow_tripdata.db",
)

asset = datasource.add_table_asset("yellow_tripdata_sample_2022_01")

batch_request = asset.build_batch_request()
batch = asset.get_batch_list_from_batch_request(batch_request)[0]

result = batch.validate(ExpectPassengerCountToBeReasonable())
print(result.describe())
