from typing import Set
from enum import Enum

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
CHECKPOINT_OPTIONAL_TOP_LEVEL_KEYS: Set[str] = {
    "evaluation_parameters",
    "profilers",
    "runtime_configuration",
}


class InteractiveFlagAttributions(Enum):
    UNPROMPTED_NULL_INTERACTIVE_FALSE_MANUAL_FALSE = (
        "unprompted_null_interactive_absent_manual_absent"
    )
    UNPROMPTED_TRUE_INTERACTIVE_TRUE_MANUAL_FALSE = (
        "unprompted_true_interactive_present_manual_absent"
    )
    UNPROMPTED_FALSE_INTERACTIVE_FALSE_MANUAL_TRUE = (
        "unprompted_false_interactive_absent_manual_present"
    )

    UNPROMPTED_TRUE_OVERRIDE_INTERACTIVE_TRUE_MANUAL_FALSE_PROFILE_TRUE = (
        "unprompted_true_override_interactive_present_manual_absent_profile_present"
    )
    UNPROMPTED_TRUE_OVERRIDE_INTERACTIVE_FALSE_MANUAL_TRUE_PROFILE_TRUE = (
        "unprompted_true_override_interactive_absent_manual_present_profile_present"
    )
    UNPROMPTED_TRUE_OVERRIDE_INTERACTIVE_FALSE_MANUAL_FALSE_BATCH_REQUEST_SPECIFIED = "unprompted_true_override_interactive_absent_manual_absent_batch_request_specified"
    UNPROMPTED_TRUE_OVERRIDE_INTERACTIVE_FALSE_MANUAL_TRUE_BATCH_REQUEST_SPECIFIED = "unprompted_true_override_interactive_absent_manual_present_batch_request_specified"
    UNPROMPTED_TRUE_OVERRIDE_INTERACTIVE_FALSE_MANUAL_FALSE_DATASOURCE_SPECIFIED = (
        "unprompted_true_override_interactive_absent_manual_absent_datasource_specified"
    )
    UNPROMPTED_TRUE_OVERRIDE_INTERACTIVE_FALSE_MANUAL_TRUE_DATASOURCE_SPECIFIED = "unprompted_true_override_interactive_absent_manual_present_datasource_specified"

    PROMPTED_CHOICE_FALSE_DEFAULT = "propmpted_choice_false_default"
    PROMPTED_CHOICE_FALSE = "prompted_choice_false"
    PROMPTED_CHOICE_TRUE_PROFILE_FALSE = "prompted_choice_true_profile_false"
    PROMPTED_CHOICE_TRUE_PROFILE_TRUE = "prompted_choice_true_profile_true"
    PROMPTED_CHOICE_TRUE = "prompted_choice_true"

    UNKNOWN = "unknown"
    ERROR = "error"
