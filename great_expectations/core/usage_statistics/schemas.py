# Test change
###
# These schemas are used to ensure that we *never* take unexpected usage stats message and provide full transparency
# about usage statistics. Please reach out to the Great Expectations with any questions!
###


# An anonymized string *must* be an md5 hash, so must have exactly 32 characters
from great_expectations.core.usage_statistics.anonymizers.types.base import (
    CLISuiteInteractiveFlagCombinations,
)

anonymized_string_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-string",
    "type": "string",
    "minLength": 32,
    "maxLength": 32,
}

anonymized_datasource_name_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-datasource-name",
    "definitions": {"anonymized_string": anonymized_string_schema},
    "anyOf": [
        {
            "type": "string",
            "maxLength": 256,
        },
        {
            "$ref": "#/definitions/anonymized_string",
        },
    ],
}

anonymized_run_time_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-run-time",
    "definitions": {"anonymized_string": anonymized_string_schema},
    "anyOf": [
        {
            "type": "string",
            "format": "date-time",
        },
        {
            "$ref": "#/definitions/anonymized_string",
        },
    ],
}

anonymized_class_info_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-class-info",
    "definitions": {"anonymized_string": anonymized_string_schema},
    "oneOf": [
        {
            "type": "object",
            "properties": {
                "anonymized_name": {"$ref": "#/definitions/anonymized_string"},
                "parent_class": {"type": "string", "maxLength": 256},
                "anonymized_class": {"$ref": "#/definitions/anonymized_string"},
            },
            "additionalProperties": True,
            # we don't want this to be true, but this is required to allow show_cta_footer
            # Note AJB-20201218 show_cta_footer was removed in v 0.9.9 via PR #1249
            "required": ["parent_class"],
        },
    ],
}

anonymized_datasource_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-datasource",
    "definitions": {
        "anonymized_string": anonymized_string_schema,
        "anonymized_class_info": anonymized_class_info_schema,
    },
    "anyOf": [
        # v2 (Batch Kwargs) API:
        {
            "type": "object",
            "properties": {
                "anonymized_name": {"$ref": "#/definitions/anonymized_string"},
                "parent_class": {"type": "string", "maxLength": 256},
                "anonymized_class": {"$ref": "#/definitions/anonymized_string"},
                "sqlalchemy_dialect": {"type": "string", "maxLength": 256},
            },
            "additionalProperties": False,
            "required": ["parent_class", "anonymized_name"],
        },
        # v3 (Batch Request) API:
        {
            "type": "object",
            "properties": {
                "anonymized_name": {"$ref": "#/definitions/anonymized_string"},
                "parent_class": {"type": "string", "maxLength": 256},
                "anonymized_class": {"$ref": "#/definitions/anonymized_string"},
                "sqlalchemy_dialect": {"type": "string", "maxLength": 256},
                "anonymized_execution_engine": {
                    "$ref": "#/definitions/anonymized_class_info"
                },
                "anonymized_data_connectors": {
                    "type": "array",
                    "maxItems": 1000,
                    "items": {"$ref": "#/definitions/anonymized_class_info"},
                },
            },
            "additionalProperties": False,
            "required": ["parent_class", "anonymized_name"],
        },
    ],
}

anonymized_store_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-store",
    "definitions": {
        "anonymized_string": anonymized_string_schema,
        "anonymized_class_info": anonymized_class_info_schema,
    },
    "oneOf": [
        {
            "type": "object",
            "properties": {
                "anonymized_name": {"$ref": "#/definitions/anonymized_string"},
                "parent_class": {"type": "string", "maxLength": 256},
                "anonymized_class": {"$ref": "#/definitions/anonymized_string"},
                "anonymized_store_backend": {
                    "$ref": "#/definitions/anonymized_class_info"
                },
            },
            "additionalProperties": False,
            "required": ["parent_class", "anonymized_name"],
        },
    ],
}

anonymized_action_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-action",
    "definitions": {"anonymized_string": anonymized_string_schema},
    "oneOf": [
        {
            "type": "object",
            "properties": {
                "anonymized_name": {"$ref": "#/definitions/anonymized_string"},
                "parent_class": {"type": "string", "maxLength": 256},
                "anonymized_class": {"$ref": "#/definitions/anonymized_string"},
            },
            "additionalProperties": False,
            "required": ["parent_class", "anonymized_name"],
        },
    ],
}

anonymized_action_list_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-action-list",
    "definitions": {"anonymized_action": anonymized_action_schema},
    "type": "array",
    "maxItems": 1000,
    "items": {"$ref": "#/definitions/anonymized_action"},
}

anonymized_validation_operator_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-validation-operator",
    "definitions": {
        "anonymized_string": anonymized_string_schema,
        "anonymized_action_list": anonymized_action_list_schema,
    },
    "oneOf": [
        {
            "type": "object",
            "properties": {
                "anonymized_name": {"$ref": "#/definitions/anonymized_string"},
                "parent_class": {"type": "string", "maxLength": 256},
                "anonymized_class": {"$ref": "#/definitions/anonymized_string"},
                "anonymized_action_list": {
                    "$ref": "#/definitions/anonymized_action_list"
                },
            },
            "additionalProperties": False,
            "required": ["parent_class", "anonymized_name"],
        },
    ],
}

empty_payload_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "empty-payload",
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}

anonymized_data_docs_site_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-data-docs-site",
    "definitions": {
        "anonymized_string": anonymized_string_schema,
        "anonymized_class_info": anonymized_class_info_schema,
    },
    "oneOf": [
        {
            "type": "object",
            "properties": {
                "anonymized_name": {"$ref": "#/definitions/anonymized_string"},
                "parent_class": {"type": "string", "maxLength": 256},
                "anonymized_class": {"$ref": "#/definitions/anonymized_string"},
                "anonymized_store_backend": {
                    "$ref": "#/definitions/anonymized_class_info"
                },
                "anonymized_site_index_builder": {
                    "$ref": "#/definitions/anonymized_class_info"
                },
            },
            "additionalProperties": False,
            "required": ["parent_class", "anonymized_name"],
        },
    ],
}

anonymized_expectation_suite_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-expectation-suite",
    "definitions": {"anonymized_string": anonymized_string_schema},
    "oneOf": [
        {
            "type": "object",
            "properties": {
                "anonymized_name": {"$ref": "#/definitions/anonymized_string"},
                "expectation_count": {"type": "number"},
                "anonymized_expectation_type_counts": {"type": "object"},
            },
            "additionalProperties": False,
        },
        {
            "type": "object",
            "properties": {
                "anonymized_name": {"$ref": "#/definitions/anonymized_string"},
                "expectation_count": {"type": "number"},
                "anonymized_expectation_counts": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "expectation_type": {"type": "string"},
                            "anonymized_expectation_type": {
                                "$ref": "#/definitions/anonymized_string"
                            },
                            "count": {"type": "number"},
                        },
                    },
                },
            },
            "additionalProperties": False,
        },
    ],
}

anonymized_init_payload_schema = {
    "$schema": "https://json-schema.org/schema#",
    "title": "anonymized-init-payload",
    "definitions": {
        "anonymized_string": anonymized_string_schema,
        "anonymized_class_info": anonymized_class_info_schema,
        "anonymized_datasource": anonymized_datasource_schema,
        "anonymized_validation_operator": anonymized_validation_operator_schema,
        "anonymized_data_docs_site": anonymized_data_docs_site_schema,
        "anonymized_store": anonymized_store_schema,
        "anonymized_action": anonymized_action_schema,
        "anonymized_action_list": anonymized_action_list_schema,
        "anonymized_expectation_suite": anonymized_expectation_suite_schema,
    },
    "type": "object",
    "properties": {
        "version": {"enum": ["1.0.0"]},
        "platform.system": {"type": "string", "maxLength": 256},
        "platform.release": {"type": "string", "maxLength": 256},
        "version_info": {"type": "string", "maxLength": 256},
        "anonymized_datasources": {
            "type": "array",
            "maxItems": 1000,
            "items": {"$ref": "#/definitions/anonymized_datasource"},
        },
        "anonymized_stores": {
            "type": "array",
            "maxItems": 1000,
            "items": {"$ref": "#/definitions/anonymized_store"},
        },
        "anonymized_validation_operators": {
            "type": "array",
            "maxItems": 1000,
            "items": {"$ref": "#/definitions/anonymized_validation_operator"},
        },
        "anonymized_data_docs_sites": {
            "type": "array",
            "maxItems": 1000,
            "items": {"$ref": "#/definitions/anonymized_data_docs_site"},
        },
        "anonymized_expectation_suites": {
            "type": "array",
            "items": {"$ref": "#/definitions/anonymized_expectation_suite"},
        },
    },
    "required": [
        "platform.system",
        "platform.release",
        "version_info",
        "anonymized_datasources",
        "anonymized_stores",
        "anonymized_data_docs_sites",
        "anonymized_expectation_suites",
    ],
    "additionalProperties": False,
}

anonymized_batch_request_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-batch-request",
    "definitions": {
        "anonymized_string": anonymized_string_schema,
        "anonymized_datasource_name": anonymized_datasource_name_schema,
    },
    "type": "object",
    "properties": {
        "anonymized_batch_request_required_top_level_properties": {
            "type": "object",
            "properties": {
                "anonymized_datasource_name": {
                    "$ref": "#/definitions/anonymized_datasource_name",
                },
                "anonymized_data_connector_name": {
                    "$ref": "#/definitions/anonymized_string",
                },
                "anonymized_data_asset_name": {
                    "$ref": "#/definitions/anonymized_string",
                },
            },
            "required": [
                "anonymized_datasource_name",
                "anonymized_data_connector_name",
                "anonymized_data_asset_name",
            ],
            "additionalProperties": False,
        },
        "batch_request_optional_top_level_keys": {
            "type": "array",
            "minItems": 1,
            "maxItems": 4,
            "items": {
                "type": "string",
                "enum": [
                    "batch_identifiers",
                    "batch_spec_passthrough",
                    "data_connector_query",
                    "runtime_parameters",
                ],
            },
            "uniqueItems": True,
        },
        "data_connector_query_keys": {
            "type": "array",
            "minItems": 1,
            "maxItems": 4,
            "items": {
                "type": "string",
                "enum": [
                    "batch_filter_parameters",
                    "custom_filter_function",
                    "index",
                    "limit",
                ],
            },
            "uniqueItems": True,
        },
        "runtime_parameters_keys": {
            "type": "array",
            "minItems": 1,
            "maxItems": 1,
            "items": {
                "type": "string",
                "enum": [
                    "batch_data",
                    "query",
                    "path",
                ],
            },
        },
        "batch_spec_passthrough_keys": {
            "type": "array",
            "minItems": 1,
            "maxItems": 6,
            "items": {
                "type": "string",
                "enum": [
                    "reader_method",
                    "reader_options",
                    "sampling_method",
                    "sampling_kwargs",
                    "splitter_method",
                    "splitter_kwargs",
                ],
            },
            "uniqueItems": True,
        },
    },
    "additionalProperties": False,
}

anonymized_batch_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-batch",
    "definitions": {
        "anonymized_string": anonymized_string_schema,
        "anonymized_datasource_name": anonymized_datasource_name_schema,
    },
    "oneOf": [
        {
            "type": "object",
            "properties": {
                "anonymized_batch_kwarg_keys": {
                    "type": "array",
                    "maxItems": 1000,
                    "items": {"oneOf": [{"type": "string", "maxLength": 256}]},
                },
                "anonymized_expectation_suite_name": {
                    "$ref": "#/definitions/anonymized_string"
                },
                "anonymized_datasource_name": {
                    "$ref": "#/definitions/anonymized_datasource_name",
                },
            },
            "additionalProperties": False,
            "required": [
                "anonymized_batch_kwarg_keys",
                "anonymized_expectation_suite_name",
                "anonymized_datasource_name",
            ],
        },
    ],
}

anonymized_run_validation_operator_payload_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-run-validation-operator-payload",
    "definitions": {
        "anonymized_string": anonymized_string_schema,
        "anonymized_batch": anonymized_batch_schema,
    },
    "type": "object",
    "properties": {
        "anonymized_operator_name": {"$ref": "#/definitions/anonymized_string"},
        "anonymized_batches": {
            "type": "array",
            "maxItems": 1000,
            "items": {"$ref": "#/definitions/anonymized_batch"},
        },
    },
    "required": ["anonymized_operator_name"],
    "additionalProperties": False,
}

anonymized_validation_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-validation",
    "definitions": {
        "anonymized_string": anonymized_string_schema,
        "anonymized_batch_request": anonymized_batch_request_schema,
        "anonymized_action_list": anonymized_action_list_schema,
    },
    "type": "object",
    "properties": {
        "anonymized_batch_request": {"$ref": "#/definitions/anonymized_batch_request"},
        "anonymized_expectation_suite_name": {
            "$ref": "#/definitions/anonymized_string"
        },
        "anonymized_action_list": {"$ref": "#/definitions/anonymized_action_list"},
    },
    "additionalProperties": False,
}

anonymized_validations_list_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-validations",
    "definitions": {"anonymized_validation": anonymized_validation_schema},
    "type": "array",
    "maxItems": 1000,
    "items": {"$ref": "#/definitions/anonymized_validation"},
}

anonymized_save_or_edit_expectation_suite_payload_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-save-or-edit-expectation-suite-payload",
    "definitions": {"anonymized_string": anonymized_string_schema},
    "type": "object",
    "properties": {
        "anonymized_expectation_suite_name": {
            "$ref": "#/definitions/anonymized_string"
        },
    },
    "required": ["anonymized_expectation_suite_name"],
    "additionalProperties": False,
}

anonymized_cli_suite_expectation_suite_payload_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-cli-suite-expectation-suite-payload",
    "definitions": {"anonymized_string": anonymized_string_schema},
    "type": "object",
    "properties": {
        "interactive_flag": {
            "type": ["boolean", "null"],
        },
        "interactive_attribution": {
            "enum": [
                element.value["interactive_attribution"]
                for element in CLISuiteInteractiveFlagCombinations
            ],
        },
        "api_version": {"type": "string", "maxLength": 256},
        "cancelled": {
            "type": ["boolean", "null"],
        },
    },
}

anonymized_cli_suite_new_expectation_suite_payload_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-cli-suite-new-expectation-suite-payload",
    "definitions": {
        "anonymized_string": anonymized_string_schema,
        "anonymized_cli_suite_expectation_suite_payload": anonymized_cli_suite_expectation_suite_payload_schema,
    },
    "items": {
        "$ref": "#/definitions/anonymized_cli_suite_expectation_suite_payload",
        "properties": {
            "anonymized_expectation_suite_name": {
                "$ref": "#/definitions/anonymized_string"
            },
        },
        "additionalProperties": False,
    },
}

anonymized_cli_suite_edit_expectation_suite_payload_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-cli-suite-edit-expectation-suite-payload",
    "definitions": {
        "anonymized_string": anonymized_string_schema,
        "anonymized_cli_suite_expectation_suite_payload": anonymized_cli_suite_expectation_suite_payload_schema,
    },
    "items": {
        "$ref": "#/definitions/anonymized_cli_suite_expectation_suite_payload",
        "properties": {
            "anonymized_expectation_suite_name": {
                "$ref": "#/definitions/anonymized_string"
            },
        },
        "required": ["anonymized_expectation_suite_name"],
        "additionalProperties": False,
    },
}

anonymized_cli_payload_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-cli-payload",
    "type": "object",
    "properties": {
        "api_version": {
            "type": "string",
            "maxLength": 256,
        },
        "cancelled": {
            "type": ["boolean", "null"],
        },
    },
    "additionalProperties": False,
}

anonymized_cli_new_ds_choice_payload_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-cli-new-ds-choice-payload",
    "type": "object",
    "properties": {
        "type": {"type": "string", "maxLength": 256},
        "db": {"type": "string", "maxLength": 256},
        "api_version": {"type": "string", "maxLength": 256},
    },
    "required": ["type"],
    "additionalProperties": False,
}


anonymized_datasource_sqlalchemy_connect_payload_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-datasource-sqlalchemy-connect-payload",
    "type": "object",
    "properties": {
        "anonymized_name": {"type": "string", "maxLength": 256},
        "sqlalchemy_dialect": {"type": "string", "maxLength": 256},
    },
    "required": ["anonymized_name"],
    "additionalProperties": False,
}

anonymized_test_yaml_config_payload_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-test-yaml-config-payload",
    "definitions": {
        "anonymized_string": anonymized_string_schema,
        "anonymized_class_info": anonymized_class_info_schema,
    },
    "type": "object",
    "properties": {
        "anonymized_name": {"$ref": "#/definitions/anonymized_string"},
        "parent_class": {"type": "string", "maxLength": 256},
        "anonymized_class": {"$ref": "#/definitions/anonymized_string"},
        "diagnostic_info": {
            "type": "array",
            "maxItems": 1000,
            "items": {
                "enum": [
                    "__substitution_error__",
                    "__yaml_parse_error__",
                    "__custom_subclass_not_core_ge__",
                    "__class_name_not_provided__",
                ],
            },
        },
        # Store
        "anonymized_store_backend": {"$ref": "#/definitions/anonymized_class_info"},
        # Datasource v2 (Batch Kwargs) API & v3 (Batch Request) API:
        "sqlalchemy_dialect": {"type": "string", "maxLength": 256},
        # Datasource v3 (Batch Request) API only:
        "anonymized_execution_engine": {"$ref": "#/definitions/anonymized_class_info"},
        "anonymized_data_connectors": {
            "type": "array",
            "maxItems": 1000,
            "items": {"$ref": "#/definitions/anonymized_class_info"},
        },
    },
    "additionalProperties": False,
}

anonymized_checkpoint_run_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-checkpoint-run-payload",
    "definitions": {
        "empty_payload": empty_payload_schema,
        "anonymized_string": anonymized_string_schema,
        "anonymized_datasource_name": anonymized_datasource_name_schema,
        "anonymized_run_time": anonymized_run_time_schema,
        "anonymized_batch_request": anonymized_batch_request_schema,
        "anonymized_action": anonymized_action_schema,
        "anonymized_action_list": anonymized_action_list_schema,
        "anonymized_validation": anonymized_validation_schema,
        "anonymized_validations": anonymized_validations_list_schema,
    },
    "oneOf": [
        {
            "type": "object",
            "properties": {
                "anonymized_name": {"$ref": "#/definitions/anonymized_string"},
                "config_version": {"type": "number", "minimum": 1},
                "anonymized_template_name": {"$ref": "#/definitions/anonymized_string"},
                "anonymized_run_name_template": {
                    "$ref": "#/definitions/anonymized_string"
                },
                "anonymized_expectation_suite_name": {
                    "$ref": "#/definitions/anonymized_string"
                },
                "anonymized_batch_request": {
                    "$ref": "#/definitions/anonymized_batch_request"
                },
                "anonymized_action_list": {
                    "$ref": "#/definitions/anonymized_action_list"
                },
                "anonymized_validations": {
                    "$ref": "#/definitions/anonymized_validations"
                },
                "anonymized_run_id": {"$ref": "#/definitions/anonymized_string"},
                "anonymized_run_name": {"$ref": "#/definitions/anonymized_run_name"},
                "anonymized_run_time": {"$ref": "#/definitions/anonymized_run_time"},
                "anonymized_expectation_suite_ge_cloud_id": {
                    "$ref": "#/definitions/anonymized_string"
                },
                "checkpoint_optional_top_level_keys": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 3,
                    "items": {
                        "type": "string",
                        "enum": [
                            "evaluation_parameters",
                            "runtime_configuration",
                            "profilers",
                        ],
                    },
                    "uniqueItems": True,
                },
            },
            "required": ["anonymized_name", "config_version"],
            "additionalProperties": False,
        },
        {"$ref": "#/definitions/empty_payload"},
    ],
}

anonymized_legacy_profiler_build_suite_payload_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-legacy-profiler-build-suite-payload",
    "type": "object",
    "properties": {
        "profile_dataset_type": {"type": "string", "maxLength": 256},
        "excluded_expectations_specified": {
            "type": ["boolean"],
        },
        "ignored_columns_specified": {
            "type": ["boolean"],
        },
        "not_null_only": {
            "type": ["boolean"],
        },
        "primary_or_compound_key_specified": {
            "type": ["boolean"],
        },
        "semantic_types_dict_specified": {
            "type": ["boolean"],
        },
        "table_expectations_only": {
            "type": ["boolean"],
        },
        "value_set_threshold_specified": {
            "type": ["boolean"],
        },
        "api_version": {"type": "string", "maxLength": 256},
    },
    "required": [
        "profile_dataset_type",
        "excluded_expectations_specified",
        "ignored_columns_specified",
        "not_null_only",
        "primary_or_compound_key_specified",
        "semantic_types_dict_specified",
        "table_expectations_only",
        "value_set_threshold_specified",
        "api_version",
    ],
    "additionalProperties": False,
}

anonymized_usage_statistics_record_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "anonymized-usage-statistics-record",
    "definitions": {
        "anonymized_string": anonymized_string_schema,
        "anonymized_datasource_name": anonymized_datasource_name_schema,
        "anonymized_datasource": anonymized_datasource_schema,
        "anonymized_store": anonymized_store_schema,
        "anonymized_class_info": anonymized_class_info_schema,
        "anonymized_validation_operator": anonymized_validation_operator_schema,
        "anonymized_action": anonymized_action_schema,
        "anonymized_action_list": anonymized_action_list_schema,
        "empty_payload": empty_payload_schema,
        "anonymized_init_payload": anonymized_init_payload_schema,
        "anonymized_run_validation_operator_payload": anonymized_run_validation_operator_payload_schema,
        "anonymized_data_docs_site": anonymized_data_docs_site_schema,
        "anonymized_batch_request": anonymized_batch_request_schema,
        "anonymized_batch": anonymized_batch_schema,
        "anonymized_expectation_suite": anonymized_expectation_suite_schema,
        "anonymized_save_or_edit_expectation_suite_payload": anonymized_save_or_edit_expectation_suite_payload_schema,
        "anonymized_cli_suite_expectation_suite_payload": anonymized_cli_suite_expectation_suite_payload_schema,
        "anonymized_cli_suite_new_expectation_suite_payload": anonymized_cli_suite_new_expectation_suite_payload_schema,
        "anonymized_cli_suite_edit_expectation_suite_payload": anonymized_cli_suite_edit_expectation_suite_payload_schema,
        "anonymized_cli_payload": anonymized_cli_payload_schema,
        "anonymized_cli_new_ds_choice_payload": anonymized_cli_new_ds_choice_payload_schema,
        "anonymized_datasource_sqlalchemy_connect_payload": anonymized_datasource_sqlalchemy_connect_payload_schema,
        "anonymized_test_yaml_config_payload": anonymized_test_yaml_config_payload_schema,
        "anonymized_validation": anonymized_validation_schema,
        "anonymized_validations": anonymized_validations_list_schema,
        "anonymized_checkpoint_run": anonymized_checkpoint_run_schema,
        "anonymized_legacy_profiler_build_suite_payload": anonymized_legacy_profiler_build_suite_payload_schema,
    },
    "type": "object",
    "properties": {
        "version": {"enum": ["1.0.0"]},
        "ge_version": {"type": "string", "maxLength": 32},
        "data_context_id": {"type": "string", "format": "uuid"},
        "data_context_instance_id": {"type": "string", "format": "uuid"},
        "event_time": {"type": "string", "format": "date-time"},
        "event_duration": {"type": "number"},
        "x-forwarded-for": {"type": "string"},
        "success": {"type": ["boolean", "null"]},
    },
    "oneOf": [
        {
            "type": "object",
            "properties": {
                "event": {"enum": ["data_context.__init__"]},
                "event_payload": {"$ref": "#/definitions/anonymized_init_payload"},
            },
        },
        {
            "type": "object",
            "properties": {
                "event": {"enum": ["data_context.save_expectation_suite"]},
                "event_payload": {
                    "$ref": "#/definitions/anonymized_save_or_edit_expectation_suite_payload"
                },
            },
        },
        {
            "type": "object",
            "properties": {
                "event": {"enum": ["data_context.run_validation_operator"]},
                "event_payload": {
                    "$ref": "#/definitions/anonymized_run_validation_operator_payload"
                },
            },
        },
        {
            "type": "object",
            "properties": {
                "event": {"enum": ["data_context.get_batch_list"]},
                "event_payload": {"$ref": "#/definitions/anonymized_batch_request"},
            },
        },
        {
            "type": "object",
            "properties": {
                "event": {"enum": ["data_asset.validate"]},
                "event_payload": {"$ref": "#/definitions/anonymized_batch"},
            },
        },
        {
            "type": "object",
            "properties": {
                "event": {"enum": ["cli.new_ds_choice"]},
                "event_payload": {
                    "$ref": "#/definitions/anonymized_cli_new_ds_choice_payload"
                },
            },
        },
        {
            "type": "object",
            "properties": {
                "event": {"enum": ["data_context.add_datasource"]},
                "event_payload": {"$ref": "#/definitions/anonymized_datasource"},
            },
        },
        {
            "type": "object",
            "properties": {
                "event": {"enum": ["datasource.sqlalchemy.connect"]},
                "event_payload": {
                    "$ref": "#/definitions/anonymized_datasource_sqlalchemy_connect_payload"
                },
            },
        },
        {
            "type": "object",
            "properties": {
                "event": {
                    "enum": [
                        "data_context.build_data_docs",
                        "data_context.open_data_docs",
                        "data_context.run_checkpoint",
                    ],
                },
                "event_payload": {"$ref": "#/definitions/empty_payload"},
            },
        },
        {
            "type": "object",
            "properties": {
                "event": {
                    "enum": ["data_context.test_yaml_config"],
                },
                "event_payload": {
                    "$ref": "#/definitions/anonymized_test_yaml_config_payload"
                },
            },
        },
        {
            "type": "object",
            "properties": {
                "event": {
                    "enum": [
                        "cli.suite.new",
                        "cli.suite.new.begin",
                        "cli.suite.new.end",
                    ],
                },
                "event_payload": {
                    "$ref": "#/definitions/anonymized_cli_suite_new_expectation_suite_payload"
                },
            },
        },
        {
            "type": "object",
            "properties": {
                "event": {
                    "enum": [
                        "cli.suite.edit",
                        "cli.suite.edit.begin",
                        "cli.suite.edit.end",
                    ],
                },
                "event_payload": {
                    "$ref": "#/definitions/anonymized_cli_suite_edit_expectation_suite_payload"
                },
            },
        },
        {
            "type": "object",
            "properties": {
                "event": {
                    "enum": ["checkpoint.run"],
                },
                "event_payload": {"$ref": "#/definitions/anonymized_checkpoint_run"},
            },
        },
        {
            "type": "object",
            "properties": {
                "event": {
                    "enum": ["legacy_profiler.build_suite"],
                },
                "event_payload": {
                    "$ref": "#/definitions/anonymized_legacy_profiler_build_suite_payload"
                },
            },
        },
        {
            "type": "object",
            "properties": {
                "event": {
                    "enum": [
                        "cli.checkpoint.delete",
                        "cli.checkpoint.delete.begin",
                        "cli.checkpoint.delete.end",
                        "cli.checkpoint.list",
                        "cli.checkpoint.list.begin",
                        "cli.checkpoint.list.end",
                        "cli.checkpoint.new",
                        "cli.checkpoint.new.begin",
                        "cli.checkpoint.new.end",
                        "cli.checkpoint.run",
                        "cli.checkpoint.run.begin",
                        "cli.checkpoint.run.end",
                        "cli.checkpoint.script",
                        "cli.checkpoint.script.begin",
                        "cli.checkpoint.script.end",
                        "cli.datasource.list",
                        "cli.datasource.list.begin",
                        "cli.datasource.list.end",
                        "cli.datasource.new",
                        "cli.datasource.new.begin",
                        "cli.datasource.new.end",
                        "cli.datasource.delete",
                        "cli.datasource.delete.begin",
                        "cli.datasource.delete.end",
                        "cli.datasource.profile",
                        "cli.docs.build",
                        "cli.docs.build.begin",
                        "cli.docs.build.end",
                        "cli.docs.clean",
                        "cli.docs.clean.begin",
                        "cli.docs.clean.end",
                        "cli.docs.list",
                        "cli.docs.list.begin",
                        "cli.docs.list.end",
                        "cli.init.create",
                        "cli.project.check_config",
                        "cli.project.upgrade.begin",
                        "cli.project.upgrade.end",
                        "cli.store.list",
                        "cli.store.list.begin",
                        "cli.store.list.end",
                        "cli.suite.delete",
                        "cli.suite.delete.begin",
                        "cli.suite.delete.end",
                        "cli.suite.demo",
                        "cli.suite.demo.begin",
                        "cli.suite.demo.end",
                        "cli.suite.list",
                        "cli.suite.list.begin",
                        "cli.suite.list.end",
                        "cli.suite.scaffold",
                        "cli.validation_operator.list",
                        "cli.validation_operator.run",
                    ],
                },
                "event_payload": {"$ref": "#/definitions/anonymized_cli_payload"},
            },
        },
    ],
    "required": [
        "version",
        "event_time",
        "data_context_id",
        "data_context_instance_id",
        "ge_version",
        "event",
        "success",
        "event_payload",
    ],
}

if __name__ == "__main__":
    import json

    with open("usage_statistics_record_schema.json", "w") as outfile:
        json.dump(anonymized_usage_statistics_record_schema, outfile, indent=2)
