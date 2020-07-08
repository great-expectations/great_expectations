batch.save_expectation_suite(discard_failed_expectations=False)
run_id = {
    "run_name": "some_string_that_uniquely_identifies_this_run",  # insert your own run_name here
    "run_time": datetime.datetime.now(datetime.timezone.utc),
}
results = context.run_validation_operator(
    "{{ validation_operator_name }}", assets_to_validate=[batch], run_id=run_id
)
validation_result_identifier = results.list_validation_result_identifiers()[0]
context.build_data_docs(site_names=["{{ site_name }}"])
context.open_data_docs(validation_result_identifier, site_name="{{ site_name }}")
