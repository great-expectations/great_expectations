from typing import Set

GETTING_STARTED_DATASOURCE_NAME: str = "getting_started_datasource"
GETTING_STARTED_EXPECTATION_SUITE_NAME: str = (
    "getting_started_expectation_suite_taxi.demo"
)
GETTING_STARTED_CHECKPOINT_NAME: str = "getting_started_checkpoint"

BATCH_REQUEST_REQUIRED_TOP_LEVEL_KEYS: Set[str] = {
    "datasource_name",
    "data_connector_name",
    "data_asset_name",
}
BATCH_REQUEST_OPTIONAL_TOP_LEVEL_KEYS: Set[str] = {
    "data_connector_query",
    "runtime_parameters",
    "batch_identifiers",
    "batch_spec_passthrough",
}
DATA_CONNECTOR_QUERY_KEYS: Set[str] = {
    "batch_filter_parameters",
    "limit",
    "index",
    "custom_filter_function",
}
RUNTIME_PARAMETERS_KEYS: Set[str] = {
    "batch_data",
    "query",
    "path",
}
BATCH_SPEC_PASSTHROUGH_KEYS: Set[str] = {
    "sampling_method",
    "sampling_kwargs",
    "splitter_method",
    "splitter_kwargs",
    "reader_method",
    "reader_options",
}
BATCH_REQUEST_FLATTENED_KEYS: Set[str] = set().union(
    *[
        BATCH_REQUEST_REQUIRED_TOP_LEVEL_KEYS,
        BATCH_REQUEST_OPTIONAL_TOP_LEVEL_KEYS,
        DATA_CONNECTOR_QUERY_KEYS,
        RUNTIME_PARAMETERS_KEYS,
        BATCH_SPEC_PASSTHROUGH_KEYS,
    ]
)
