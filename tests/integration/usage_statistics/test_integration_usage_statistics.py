import copy
import datetime
import os
import signal
import subprocess
import time
import uuid

import boto3
import botocore
import pytest
import requests

from great_expectations.data_context.util import file_relative_path

USAGE_STATISTICS_QA_URL = (
    "https://qa.stats.greatexpectations.io/great_expectations/v1/usage_statistics"
)

logGroupName = "/great_expectations/usage_statistics/qa"


@pytest.fixture(scope="session")
def aws_session():
    aws_session = None
    for session_options in [
        {"profile_name": "travis-ci", "region_name": "us-east-1"},
        {"profile_name": "default", "region_name": "us-east-1"},
        {"region_name": "us-east-1"},
    ]:
        try:
            aws_session = boto3.Session(**session_options)
        except botocore.exceptions.ProfileNotFound:
            continue
    return aws_session


@pytest.fixture(scope="session")
def valid_usage_statistics_message():
    return {
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
        "ge_version": "0.9.7+244.g56d67e51d.dirty",
    }


def test_send_malformed_data(valid_usage_statistics_message):
    # We should be able to successfully send a valid message, but find that
    # a malformed message is not accepted
    res = requests.post(USAGE_STATISTICS_QA_URL, json=valid_usage_statistics_message)
    assert res.status_code == 201
    invalid_usage_statistics_message = copy.deepcopy(valid_usage_statistics_message)
    del invalid_usage_statistics_message["data_context_id"]
    res = requests.post(USAGE_STATISTICS_QA_URL, json=invalid_usage_statistics_message)
    assert res.status_code == 400


def test_graceful_failure_with_no_internet():
    """Test that having usage statistics enabled does not negatively impact kill signals or cause loss of queued usage statistics. """

    # Execute process that initializes data context
    # NOTE - JPC - 20200227 - this is crazy long (not because of logging I think, but worth revisiting)
    acceptable_startup_time = 6
    acceptable_shutdown_time = 1
    nap_time = 0
    start = datetime.datetime.now()
    data_context_id = str(uuid.uuid4())
    # Instruct the process to wait for 30 seconds after initializing before completing.
    p = subprocess.Popen(
        [
            "python",
            file_relative_path(
                __file__, "./instantiate_context_with_usage_statistics.py"
            ),
            data_context_id,
            str(nap_time),
            "True",
            "True",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    outs, errs = p.communicate()
    end = datetime.datetime.now()
    # We didn't wait or send a signal, so just check that times were reasonable
    assert (end - start) < datetime.timedelta(
        seconds=acceptable_startup_time + acceptable_shutdown_time
    )
    outs = str(outs)
    errs = str(errs)
    assert "INFO" not in outs
    assert "Done constructing a DataContext" in outs
    assert "Ending a long nap" in outs
