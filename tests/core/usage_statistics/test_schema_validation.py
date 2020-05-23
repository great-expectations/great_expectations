import jsonschema
import pytest

from great_expectations.core.usage_statistics.schemas import (
    anonymized_datasource_schema,
    anonymized_string_schema,
    init_payload_schema,
    usage_statistics_record_schema,
)


def test_anonymized_name_validation():
    string = "aa41efe0a1b3eeb9bf303e4561ff8392"
    jsonschema.validate(string, anonymized_string_schema)

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(string[:5], anonymized_string_schema)


def test_anonymized_datasource_validation():
    record = {
        "anonymized_name": "aa41efe0a1b3eeb9bf303e4561ff8392",
        "parent_class": "hello",
    }
    jsonschema.validate(record, anonymized_datasource_schema)

    record = {
        "anonymized_name": "aa41efe0a1b3eeb9bf303e4561ff8392",
        "parent_class": "hello",
        "anonymized_class": "aa41efe0a1b3eeb9bf303e4561ff8392",
    }
    jsonschema.validate(record, anonymized_datasource_schema)
    record = {
        "anonymized_name": "aa41efe0a1b3eeb9bf303e4561ff8392",
        "parent_class": "SparkDFDataset",
        "anonymized_class": "aa41efe0a1b3eeb9bf303e4561ff8392",
        "sqlalchemy_dialect": "postgres",
    }
    jsonschema.validate(record, anonymized_datasource_schema)


def test_init_payload_validation():
    payload = {
        "platform.system": "Darwin",
        "platform.release": "19.3.0",
        "version_info": "sys.version_info(major=3, minor=7, micro=4, releaselevel='final', serial=0)",
        "anonymized_datasources": [
            {
                "anonymized_name": "f57d8a6edae4f321b833384801847498",
                "parent_class": "SqlAlchemyDatasource",
                "sqlalchemy_dialect": "postgresql",
            }
        ],
        "anonymized_stores": [
            {
                "anonymized_name": "078eceafc1051edf98ae2f911484c7f7",
                "parent_class": "ExpectationsStore",
                "anonymized_store_backend": {
                    "parent_class": "TupleFilesystemStoreBackend"
                },
            },
            {
                "anonymized_name": "313cbd9858dd92f3fc2ef1c10ab9c7c8",
                "parent_class": "ValidationsStore",
                "anonymized_store_backend": {
                    "parent_class": "TupleFilesystemStoreBackend"
                },
            },
            {
                "anonymized_name": "2d487386aa7b39e00ed672739421473f",
                "parent_class": "EvaluationParameterStore",
                "anonymized_store_backend": {"parent_class": "InMemoryStoreBackend"},
            },
        ],
        "anonymized_validation_operators": [
            {
                "anonymized_name": "99d14cc00b69317551690fb8a61aca94",
                "parent_class": "ActionListValidationOperator",
                "anonymized_action_list": [
                    {
                        "anonymized_name": "5a170e5b77c092cc6c9f5cf2b639459a",
                        "parent_class": "StoreValidationResultAction",
                    },
                    {
                        "anonymized_name": "0fffe1906a8f2a5625a5659a848c25a3",
                        "parent_class": "StoreEvaluationParametersAction",
                    },
                    {
                        "anonymized_name": "101c746ab7597e22b94d6e5f10b75916",
                        "parent_class": "UpdateDataDocsAction",
                    },
                ],
            }
        ],
        "anonymized_data_docs_sites": [
            {
                "parent_class": "SiteBuilder",
                "anonymized_name": "eaf0cf17ad63abf1477f7c37ad192700",
                "anonymized_store_backend": {
                    "parent_class": "TupleFilesystemStoreBackend"
                },
                "anonymized_site_index_builder": {
                    "parent_class": "DefaultSiteIndexBuilder",
                    "show_cta_footer": True,
                },
            }
        ],
        "anonymized_expectation_suites": [
            {
                "anonymized_name": "238e99998c7674e4ff26a9c529d43da4",
                "expectation_count": 8,
                "anonymized_expectation_type_counts": {
                    "expect_column_value_lengths_to_be_between": 1,
                    "expect_table_row_count_to_be_between": 1,
                    "expect_column_values_to_not_be_null": 2,
                    "expect_column_distinct_values_to_be_in_set": 1,
                    "expect_column_kl_divergence_to_be_less_than": 1,
                    "expect_table_column_count_to_equal": 1,
                    "expect_table_columns_to_match_ordered_list": 1,
                },
            }
        ],
    }
    jsonschema.validate(payload, init_payload_schema)


def test_run_val_op_message():
    message = {
        "event_payload": {
            "anonymized_operator_name": "50daa62a8739db21009f452f7e36153b",
        },
        "event": "data_context.run_validation_operator",
        "success": True,
        "version": "1.0.0",
        "event_time": "2020-03-26T23:02:17.932Z",
        "data_context_id": "705dd2a2-27f8-470f-9ebe-e7058fd7a534",
        "data_context_instance_id": "4f6deb55-8fbd-4131-9f97-b42b0902eae5",
        "ge_version": "0.9.7+203.ge3a97f44.dirty",
    }
    jsonschema.validate(message, usage_statistics_record_schema)
