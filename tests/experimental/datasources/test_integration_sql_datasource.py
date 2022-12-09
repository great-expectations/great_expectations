import pytest

from great_expectations import DataContext
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.data_context.util import file_relative_path


@pytest.mark.integration
def test_run_checkpoint_and_data_doc(empty_data_context):
    db_file = file_relative_path(
        __file__,
        "../../test_sets/taxi_yellow_tripdata_samples/sqlite/yellow_tripdata.db")
    context: DataContext = empty_data_context

    # Add sqlalchemy datasource.
    # The current method is called `add_postgres` it really should be `add_sql`.
    # See https://superconductive.atlassian.net/browse/GREAT-1400
    datasource = context.sources.add_postgres(
        name="test_datasource",
        connection_string=f"sqlite:///{db_file}",
    )

    # Add and configure a data asset
    table = "yellow_tripdata_sample_2019_01"
    split_col = "pickup_datetime"
    asset = datasource.add_table_asset(
        name="my_asset",
        table_name=table,
    ).add_year_and_month_splitter(
        column_name=split_col
    ).add_sorters(["year", "month"])

    # Define an expectation suite
    suite_name = "my_suite"
    context.create_expectation_suite(
        expectation_suite_name=suite_name
    )
    batch_request = asset.get_batch_request({"year": 2019, "month": 1})
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )
    validator.expect_table_row_count_to_be_between(0, 10000)
    validator.expect_column_max_to_be_between(column="passenger_count", min_value=1, max_value=7)
    validator.expect_column_median_to_be_between(column="passenger_count", min_value=1, max_value=4)
    validator.save_expectation_suite(discard_failed_expectations=False)

    # Configure and run a checkpoint
    checkpoint_config = {
        "class_name": "SimpleCheckpoint",
        "validations": [{
            "batch_request": batch_request, "expectation_suite_name": suite_name
        }]
    }
    metadata = validator.active_batch.metadata
    checkpoint = SimpleCheckpoint(
        f"batch_with_year_{metadata['year']}_month_{metadata['month']}_{suite_name}",
        context,
        **checkpoint_config
    )
    checkpoint_result = checkpoint.run()

    # Verify checkpoint runs successfully
    assert checkpoint_result._success, "Running expectation suite failed"
    assert len(checkpoint_result.run_results) == 1, "More than 1 run was done when we only expected 1"

    # Grab the validation result and verify it is correct
    result = checkpoint_result["run_results"][list(checkpoint_result["run_results"].keys())[0]]
    validation_result = result["validation_result"]
    assert validation_result.success

    expected_metric_values = {
        'expect_table_row_count_to_be_between': 10000,
        "expect_column_max_to_be_between": 6,
        "expect_column_median_to_be_between": 1,
    }
    assert len(validation_result.results) == 3

    for r in validation_result.results:
        assert r.success
        assert r.result["observed_value"] == expected_metric_values[r.expectation_config.expectation_type]

    # Rudimentary test for data doc generation
    docs_dict = context.build_data_docs()
    assert "local_site" in docs_dict, "build_data_docs returned dictionary has changed"
    assert docs_dict["local_site"].startswith("file://"), "build_data_docs returns file path in unexpected form"
    path = docs_dict["local_site"][7:]
    with open(path) as f:
        data_doc_index = f.read()

    # Checking for ge-success-icon tests the result table was generated and it was populated with a successful run.
    assert 'ge-success-icon' in data_doc_index
    assert 'ge-failed-icon' not in data_doc_index

