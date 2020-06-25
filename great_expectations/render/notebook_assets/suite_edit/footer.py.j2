batch.save_expectation_suite(discard_failed_expectations=False)

"""
Let's create a run_id. The run_id must be of type RunIdentifier, with optional run_name and run_time instantiation
arguments (or a dictionary with these keys). The run_name can be any string (this could come from your pipeline
runner, e.g. Airflow run id). The run_time can be either a dateutil parsable string or a datetime object.
Note - any provided datetime will be assumed to be a UTC time. If no instantiation arguments are given, run_name will
be None and run_time will default to the current UTC datetime.
"""

run_id = {
  "run_name": "some_string_that_uniquely_identifies_this_run",  # insert your own run_name here
  "run_time": datetime.datetime.now(datetime.timezone.utc)
}

results = context.run_validation_operator("action_list_operator", assets_to_validate=[batch], run_id=run_id)
validation_result_identifier = results.list_validation_result_identifiers()[0]
context.build_data_docs()
context.open_data_docs(validation_result_identifier)
