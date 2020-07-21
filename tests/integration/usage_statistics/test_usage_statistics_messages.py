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
            "ge_version": "0.11.5.manual_test",
        }
    ],
    "cli.suite.list": [
        {
            "version": "1.0.0",
            "event_time": "2020-06-26T19:33:33.123Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.5.manual_testing",
            "event": "cli.suite.list",
            "success": True,
            "event_payload": {},
        }
    ],
    "cli.suite.new": [
        {
            "version": "1.0.0",
            "event_time": "2020-06-26T19:33:33.123Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.5.manual_testing",
            "event": "cli.suite.new",
            "success": True,
            "event_payload": {},
        }
    ],
    "cli.init.create": [
        {
            "event": "cli.init.create",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:06:47.697Z",
            "data_context_id": "df1f151d-ebb3-4a2f-81d7-14a17453cf28",
            "data_context_instance_id": "f73d1e68-3897-4bf7-bc6e-8533f27ce337",
            "ge_version": "0.11.5.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        }
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
            "ge_version": "0.11.5.manual_testing",
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
            "ge_version": "0.11.5.manual_testing",
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
            "ge_version": "0.11.5.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        }
    ],
    "data_context.save.expectation.suite": [
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
            "ge_version": "0.11.5.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
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
            "ge_version": "0.11.5.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        }
    ],
    # TWEAK TO BE LIKE ABOVE
    # "cli.store.list",
    # "cli.project.check_config",
    # "cli.validation_operator.run",
    # "cli.validation_operator.list",
    # "cli.tap.new",
    # "cli.docs.list",
    # "cli.docs.build", --> has this been changed to data_context.build_data_docs? #TODO check if this is the case
    # "cli.datasource.profile",
    # "cli.datasource.list",
    # "cli.datasource.new" --> has this been changed to cli.new_ds_choice? #TODO check if this is the case
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
