import pathlib
import great_expectations as gx

folder_path = str(
    pathlib.Path(
        gx.__file__,
        "..",
        "..",
        "tests",
        "test_sets",
        "visits",
    ).resolve(strict=True)
)
connection_string: str = f"sqlite:///{folder_path}/visits.db"

# get context
# <snippet name="tests/integration/docusaurus/expectations/advanced/failed_rows_sql.py get context">
import great_expectations as gx

context = gx.get_context(project_root_dir=".")
# </snippet>

# add datasource and asset
datasource = context.sources.add_sqlite(
    name="visits_datasource",
    connection_string=connection_string,
)
asset = datasource.add_table_asset(
    name="visits",
    table_name="event_names",
)

# get checkpoint
# <snippet name="tests/integration/docusaurus/expectations/advanced/failed_rows_sql.py get checkpoint">
my_checkpoint = context.get_checkpoint("my_checkpoint")
# </snippet>

# Example 1 - No unexpected_index_column_names. This is the default.
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
    "partial_unexpected_counts": [
        {"value": "download", "count": 1},
        {"value": "purchase", "count": 1},
        {"value": "user_signup", "count": 1},
    ],
}


# Example 2 - 1 unexpected_index_column_names defined. Output will contain unexpected_index_list and unexpected_index_query.
# <snippet name="tests/integration/docusaurus/expectations/advanced/failed_rows_sql.py set unexpected_index_column_names">
result_format: dict = {
    "result_format": "COMPLETE",
    "unexpected_index_column_names": ["event_id"],
}
# </snippet>
# <snippet name="tests/integration/docusaurus/expectations/advanced/failed_rows_sql.py run checkpoint">
results = my_checkpoint.run(result_format=result_format)
# </snippet>
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
        {"event_id": 3, "event_type": "user_signup"},
        {"event_id": 4, "event_type": "purchase"},
        {"event_id": 5, "event_type": "download"},
    ],
    "partial_unexpected_counts": [
        {"value": "download", "count": 1},
        {"value": "purchase", "count": 1},
        {"value": "user_signup", "count": 1},
    ],
    "unexpected_list": ["user_signup", "purchase", "download"],
    "unexpected_index_list": [
        {"event_id": 3, "event_type": "user_signup"},
        {"event_id": 4, "event_type": "purchase"},
        {"event_id": 5, "event_type": "download"},
    ],
    "unexpected_index_query": "SELECT event_id, event_type \nFROM event_names \nWHERE event_type IS NOT NULL AND (event_type NOT IN ('page_load', 'page_view'));",
}

# Example 3 -  2 unexpected_index_column_names defined. Output will contain unexpected_index_list and unexpected_index_query.
result_format: dict = {
    "result_format": "COMPLETE",
    "unexpected_index_column_names": ["event_id", "visit_id"],
}
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
        {"event_id": 3, "visit_id": 1470387700, "event_type": "user_signup"},
        {"event_id": 4, "visit_id": 1470438716, "event_type": "purchase"},
        {"event_id": 5, "visit_id": 1470420524, "event_type": "download"},
    ],
    "partial_unexpected_counts": [
        {"value": "download", "count": 1},
        {"value": "purchase", "count": 1},
        {"value": "user_signup", "count": 1},
    ],
    "unexpected_list": ["user_signup", "purchase", "download"],
    "unexpected_index_list": [
        {"event_id": 3, "visit_id": 1470387700, "event_type": "user_signup"},
        {"event_id": 4, "visit_id": 1470438716, "event_type": "purchase"},
        {"event_id": 5, "visit_id": 1470420524, "event_type": "download"},
    ],
    "unexpected_index_query": "SELECT event_id, visit_id, event_type \nFROM event_names \nWHERE event_type IS NOT NULL AND (event_type NOT IN ('page_load', 'page_view'));",
}
