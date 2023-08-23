import great_expectations as gx

context = gx.get_context(context_root_dir="./great_expectations", cloud_mode=False)
data_asset = context.sources.add_pandas(name="visits_datasource").add_csv_asset(
    name="visits", filepath_or_buffer="./data/visits.csv", sep="\t"
)

# get checkpoint
my_checkpoint = context.get_checkpoint("my_checkpoint")


# Example 1
results = my_checkpoint.run()
evrs = results.list_validation_results()
assert (evrs[0]["results"][0]["result"]) == {
    "element_count": 6,
    "unexpected_count": 3,
    "unexpected_percent": 50.0,
    "partial_unexpected_list": ["user_signup", "purchase", "download"],
    "missing_count": 0,
    "missing_percent": 0.0,
    "unexpected_percent_total": 50.0,
    "unexpected_percent_nonmissing": 50.0,
    "partial_unexpected_index_list": [3, 4, 5],
    "partial_unexpected_counts": [
        {"value": "download", "count": 1},
        {"value": "purchase", "count": 1},
        {"value": "user_signup", "count": 1},
    ],
}


# Example 2
result_format: dict = {
    "result_format": "COMPLETE",
    "unexpected_index_column_names": ["event_id"],
}
results = my_checkpoint.run(result_format=result_format)
evrs = results.list_validation_results()
assert (evrs[0]["results"][0]["result"]) == {
    "element_count": 6,
    "unexpected_count": 3,
    "unexpected_percent": 50.0,
    "partial_unexpected_list": ["user_signup", "purchase", "download"],
    "unexpected_index_column_names": ["event_id"],
    "missing_count": 0,
    "missing_percent": 0.0,
    "unexpected_percent_total": 50.0,
    "unexpected_percent_nonmissing": 50.0,
    "partial_unexpected_index_list": [
        {"event_type": "user_signup", "event_id": 3},
        {"event_type": "purchase", "event_id": 4},
        {"event_type": "download", "event_id": 5},
    ],
    "partial_unexpected_counts": [
        {"value": "download", "count": 1},
        {"value": "purchase", "count": 1},
        {"value": "user_signup", "count": 1},
    ],
    "unexpected_list": ["user_signup", "purchase", "download"],
    "unexpected_index_list": [
        {"event_type": "user_signup", "event_id": 3},
        {"event_type": "purchase", "event_id": 4},
        {"event_type": "download", "event_id": 5},
    ],
    "unexpected_index_query": "df.filter(items=[3, 4, 5], axis=0)",
}


# Example 3
result_format: dict = {
    "result_format": "COMPLETE",
    "unexpected_index_column_names": ["event_id", "visit_id"],
}
# context.add_or_update_checkpoint(name="my_checkpoint", batch_request=my_batch_request, expectation_suite_name="suite",  runtime_configuration=result_format)
results = my_checkpoint.run(result_format=result_format)
evrs = results.list_validation_results()
assert (evrs[0]["results"][0]["result"]) == {
    "element_count": 6,
    "unexpected_count": 3,
    "unexpected_percent": 50.0,
    "partial_unexpected_list": ["user_signup", "purchase", "download"],
    "unexpected_index_column_names": ["event_id", "visit_id"],
    "missing_count": 0,
    "missing_percent": 0.0,
    "unexpected_percent_total": 50.0,
    "unexpected_percent_nonmissing": 50.0,
    "partial_unexpected_index_list": [
        {"event_type": "user_signup", "event_id": 3, "visit_id": 1470387700},
        {"event_type": "purchase", "event_id": 4, "visit_id": 1470438716},
        {"event_type": "download", "event_id": 5, "visit_id": 1470420524},
    ],
    "partial_unexpected_counts": [
        {"value": "download", "count": 1},
        {"value": "purchase", "count": 1},
        {"value": "user_signup", "count": 1},
    ],
    "unexpected_list": ["user_signup", "purchase", "download"],
    "unexpected_index_list": [
        {"event_type": "user_signup", "event_id": 3, "visit_id": 1470387700},
        {"event_type": "purchase", "event_id": 4, "visit_id": 1470438716},
        {"event_type": "download", "event_id": 5, "visit_id": 1470420524},
    ],
    "unexpected_index_query": "df.filter(items=[3, 4, 5], axis=0)",
}
