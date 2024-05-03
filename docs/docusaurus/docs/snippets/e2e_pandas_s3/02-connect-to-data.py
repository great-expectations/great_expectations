from great_expectations import get_context
import great_expectations.expectations as gxe

context = get_context(project_root_dir="./gx")

try:
    data_source = context.datasources["nextflow"]
    # TODO: this will be updated to become
    # data_source = context.data_sources.get("nextflow")
# TODO: instead of keyerror will be ResourceNotFoundError
except KeyError:
    data_source = context.data_sources.add_pandas_s3(
        name="nextflow",
        bucket="nyc-tlc",
    )

try:
    asset = data_source.get_asset("my_project")
# TODO: instead of LookupError will be ResourceNotFoundError
except LookupError:
    asset = data_source.add_parquet_asset("my_project", s3_prefix="trip data/")

try:
    batch_definition = asset.get_batch_definition("monthly")
except KeyError:
    import re
    pattern = re.compile(r"trip data/yellow_tripdata_(?P<year>[0-9]{4})-(?P<month>[0-9]{2}).parquet")
    batch_definition = asset.add_batch_definition_monthly(
        "monthly",
        regex=pattern
    )


# To verify that things worked...
batch = batch_definition.get_batch(batch_parameters={"year": "2020", "month": "04"})

import great_expectations.expectations as gxe
print(batch.validate(gxe.ExpectColumnToExist(column="VendorID")))
