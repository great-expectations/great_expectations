"""Test usage statistics transmission client-side."""
import pytest
import requests

USAGE_STATISTICS_QA_URL = (
    "https://qa.stats.greatexpectations.io/great_expectations/v1/usage_statistics"
)


"""
valid_usage_statistics_messages should include a list of messages that we want to ensure are valid.

Whenever a new kind of message is added, an example of that message should be included here.

Each message will be sent to the server to ensure it is accepted.

"""
valid_usage_statistics_messages = {
    "data_context.__init__": [
        {
            "event_payload": {
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
                        "anonymized_store_backend": {
                            "parent_class": "InMemoryStoreBackend"
                        },
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
            },
            "event": "data_context.__init__",
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-03-28T01:14:21.155Z",
            "data_context_id": "96c547fe-e809-4f2e-b122-0dc91bb7b3ad",
            "data_context_instance_id": "445a8ad1-2bd0-45ce-bb6b-d066afe996dd",
            "ge_version": "0.11.9.manual_test",
        },
        # "new-style" expectation type system
        {
            "event_payload": {
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
                        "anonymized_store_backend": {
                            "parent_class": "InMemoryStoreBackend"
                        },
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
                        "anonymized_expectation_counts": [
                            {
                                "expectation_type": "expect_column_value_lengths_to_be_between",
                                "count": 1,
                            },
                            {
                                "expectation_type": "expect_table_row_count_to_be_between",
                                "count": 1,
                            },
                            {
                                "expectation_type": "expect_column_values_to_not_be_null",
                                "count": 2,
                            },
                            {
                                "expectation_type": "expect_column_distinct_values_to_be_in_set",
                                "count": 1,
                            },
                            {
                                "expectation_type": "expect_column_kl_divergence_to_be_less_than",
                                "count": 1,
                            },
                            {
                                "expectation_type": "expect_table_column_count_to_equal",
                                "count": 1,
                            },
                            {
                                "expectation_type": "expect_table_columns_to_match_ordered_list",
                                "count": 1,
                            },
                        ],
                    }
                ],
            },
            "event": "data_context.__init__",
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-03-28T01:14:21.155Z",
            "data_context_id": "96c547fe-e809-4f2e-b122-0dc91bb7b3ad",
            "data_context_instance_id": "445a8ad1-2bd0-45ce-bb6b-d066afe996dd",
            "ge_version": "0.13.0.manual_test",
        },
    ],
    "data_asset.validate": [
        {
            "event": "data_asset.validate",
            "event_payload": {
                "anonymized_batch_kwarg_keys": [
                    "path",
                    "datasource",
                    "data_asset_name",
                ],
                "anonymized_expectation_suite_name": "dbb859464809a03647feb14a514f12b8",
                "anonymized_datasource_name": "a41caeac7edb993cfbe55746e6a328b5",
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:36:26.422Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        }
    ],
    "data_context.add_datasource": [
        {
            "event_payload": {
                "anonymized_name": "c9633f65c36d1ba9fbaa9009c1404cfa",
                "parent_class": "PandasDatasource",
            },
            "event": "data_context.add_datasource",
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:16.030Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        }
    ],
    "data_context.build_data_docs": [
        {
            "event_payload": {},
            "event": "data_context.build_data_docs",
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:24.349Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        }
    ],
    "data_context.open_data_docs": [
        {
            "event_payload": {},
            "event": "data_context.open_data_docs",
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        }
    ],
    "data_context.save_expectation_suite": [
        {
            "event_payload": {
                "anonymized_expectation_suite_name": "4b6bf73298fcc2db6da929a8f18173f7"
            },
            "event": "data_context.save_expectation_suite",
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:23.570Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        }
    ],
    "datasource.sqlalchemy.connect": [
        {
            "event": "datasource.sqlalchemy.connect",
            "event_payload": {
                "anonymized_name": "6989a7654d0e27470dc01292b6ed0dea",
                "sqlalchemy_dialect": "postgresql",
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:38:32.664Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.5.manual_testing",
        }
    ],
    "cli.init.create": [
        {
            "event": "cli.init.create",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:06:47.697Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "cli.init.create",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:06:47.697Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "cli.init.create",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:06:47.697Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
    ],
    "cli.checkpoint.new": [
        {
            "event": "cli.checkpoint.new",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.checkpoint.new",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.checkpoint.new",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    "cli.checkpoint.script": [
        {
            "event": "cli.checkpoint.script",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.checkpoint.script",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.checkpoint.script",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    "cli.checkpoint.run": [
        {
            "event": "cli.checkpoint.run",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.checkpoint.run",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.checkpoint.run",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    "cli.checkpoint.list": [
        {
            "event": "cli.checkpoint.list",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.checkpoint.list",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.checkpoint.list",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    "cli.checkpoint.delete": [
        {
            "event": "cli.checkpoint.delete",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.checkpoint.delete",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.checkpoint.delete",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    "cli.datasource.list": [
        {
            "event": "cli.datasource.list",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:01:06.628Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.datasource.list",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:01:06.628Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.datasource.list",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:01:06.628Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    "cli.datasource.new": [
        {
            "event": "cli.datasource.new",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:38:32.701Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.datasource.new",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:38:32.701Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.datasource.new",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:38:32.701Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    "cli.datasource.profile": [
        {
            "event": "cli.datasource.profile",
            "event_payload": {},
            "success": False,
            "version": "1.0.0",
            "event_time": "2020-08-05T01:03:17.567Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.datasource.profile",
            "event_payload": {"api_version": "v2"},
            "success": False,
            "version": "1.0.0",
            "event_time": "2020-08-05T01:03:17.567Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.datasource.profile",
            "event_payload": {"api_version": "v3"},
            "success": False,
            "version": "1.0.0",
            "event_time": "2020-08-05T01:03:17.567Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    "cli.docs.build": [
        {
            "event": "cli.docs.build",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:25:27.088Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.docs.build",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:25:27.088Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.docs.build",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:25:27.088Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    "cli.docs.clean": [
        {
            "event": "cli.docs.clean",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-05T00:36:50.979Z",
            "data_context_id": "2a948908-ec42-47f2-b972-c07bb0393de4",
            "data_context_instance_id": "e7e0916d-d527-437a-b89d-5eb8c36d408f",
            "ge_version": "0.11.9+25.g3ca555c.dirty",
        },
        {
            "event": "cli.docs.clean",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-05T00:36:50.979Z",
            "data_context_id": "2a948908-ec42-47f2-b972-c07bb0393de4",
            "data_context_instance_id": "e7e0916d-d527-437a-b89d-5eb8c36d408f",
            "ge_version": "0.13.0+25.g3ca555c.dirty",
        },
        {
            "event": "cli.docs.clean",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-05T00:36:50.979Z",
            "data_context_id": "2a948908-ec42-47f2-b972-c07bb0393de4",
            "data_context_instance_id": "e7e0916d-d527-437a-b89d-5eb8c36d408f",
            "ge_version": "0.13.0+25.g3ca555c.dirty",
        },
    ],
    "cli.docs.list": [
        {
            "event": "cli.docs.list",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:20:37.828Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.docs.list",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:20:37.828Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.docs.list",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:20:37.828Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    "cli.new_ds_choice": [
        {
            "event": "cli.new_ds_choice",
            "event_payload": {"type": "pandas"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:08.963Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "cli.new_ds_choice",
            "event_payload": {"type": "pandas", "api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:08.963Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "cli.new_ds_choice",
            "event_payload": {"type": "pandas", "api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:08.963Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
    ],
    "cli.project.check_config": [
        {
            "event": "cli.project.check_config",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:42:34.068Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.project.check_config",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:42:34.068Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.project.check_config",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:42:34.068Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    "cli.store.list": [
        {
            "event": "cli.store.list",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:56:53.908Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.store.list",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:56:53.908Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.store.list",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:56:53.908Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    "cli.suite.demo": [
        {
            "event": "cli.suite.demo",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-05T00:40:41.401Z",
            "data_context_id": "2a948908-ec42-47f2-b972-c07bb0393de4",
            "data_context_instance_id": "e7e0916d-d527-437a-b89d-5eb8c36d408f",
            "ge_version": "0.11.9+25.g3ca555c.dirty",
        },
        {
            "event": "cli.suite.demo",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-05T00:40:41.401Z",
            "data_context_id": "2a948908-ec42-47f2-b972-c07bb0393de4",
            "data_context_instance_id": "e7e0916d-d527-437a-b89d-5eb8c36d408f",
            "ge_version": "0.13.0+25.g3ca555c.dirty",
        },
        {
            "event": "cli.suite.demo",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-05T00:40:41.401Z",
            "data_context_id": "2a948908-ec42-47f2-b972-c07bb0393de4",
            "data_context_instance_id": "e7e0916d-d527-437a-b89d-5eb8c36d408f",
            "ge_version": "0.13.0+25.g3ca555c.dirty",
        },
    ],
    "cli.suite.edit": [
        {
            "event": "cli.suite.edit",
            "event_payload": {
                "anonymized_expectation_suite_name": "0604e6a8f5a1da77e0438aa3b543846e"
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:16:22.149Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.suite.edit",
            "event_payload": {
                "anonymized_expectation_suite_name": "0604e6a8f5a1da77e0438aa3b543846e",
                "api_version": "v2",
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:16:22.149Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.suite.edit",
            "event_payload": {
                "anonymized_expectation_suite_name": "0604e6a8f5a1da77e0438aa3b543846e",
                "api_version": "v3",
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:16:22.149Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    "cli.suite.list": [
        {
            "event": "cli.suite.list",
            "version": "1.0.0",
            "event_payload": {},
            "success": True,
            "event_time": "2020-06-26T19:33:33.123Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.suite.list",
            "version": "1.0.0",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "event_time": "2020-06-26T19:33:33.123Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.suite.list",
            "version": "1.0.0",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "event_time": "2020-06-26T19:33:33.123Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    "cli.suite.new": [
        {
            "version": "1.0.0",
            "event_time": "2020-06-26T19:33:33.123Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
            "event": "cli.suite.new",
            "success": True,
            "event_payload": {},
        },
        {
            "version": "1.0.0",
            "event_time": "2020-06-26T19:33:33.123Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
            "event": "cli.suite.new",
            "success": True,
            "event_payload": {"api_version": "v2"},
        },
        {
            "version": "1.0.0",
            "event_time": "2020-06-26T19:33:33.123Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
            "event": "cli.suite.new",
            "success": True,
            "event_payload": {"api_version": "v3"},
        },
    ],
    "cli.suite.scaffold": [
        {
            "event": "cli.suite.scaffold",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-05T00:58:51.961Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.suite.scaffold",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-05T00:58:51.961Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.suite.scaffold",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-05T00:58:51.961Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    "cli.suite.delete": [
        {
            "event": "cli.suite.delete",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-05T00:58:51.961Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.suite.delete",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-05T00:58:51.961Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.suite.delete",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-05T00:58:51.961Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    "cli.validation_operator.list": [
        {
            "event": "cli.validation_operator.list",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:32:33.635Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.validation_operator.list",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:32:33.635Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    "cli.validation_operator.run": [
        {
            "event": "cli.validation_operator.run",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:33:15.664Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.validation_operator.run",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:33:15.664Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
}

test_messages = []
message_test_ids = []
for message_type, messages in valid_usage_statistics_messages.items():
    for idx, test_message in enumerate(messages):
        test_messages += [test_message]
        message_test_ids += [f"{message_type}_{idx}"]


@pytest.mark.aws_integration
@pytest.mark.parametrize("message", test_messages, ids=message_test_ids)
def test_usage_statistics_message(message):
    """known message formats should be valid"""
    res = requests.post(USAGE_STATISTICS_QA_URL, json=message, timeout=2)
    assert res.status_code == 201
    assert res.json() == {"event_count": 1}
