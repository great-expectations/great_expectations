import os

import boto3
import pytest
from moto import mock_sns

from great_expectations.core import ExpectationSuiteValidationResult, RunIdentifier
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.types.resource_identifiers import (
    BatchIdentifier,
    ExpectationSuiteIdentifier,
    GXCloudIdentifier,
    ValidationResultIdentifier,
)


@pytest.fixture(scope="module")
def validation_result_suite_ge_cloud_id():
    return "bfe7dc64-5320-49b0-91c1-2e8029e06c4d"


@pytest.fixture(scope="module")
def validation_result_suite():
    return ExpectationSuiteValidationResult(
        results=[],
        success=True,
        suite_name="empty_suite",
        statistics={
            "evaluated_expectations": 0,
            "successful_expectations": 0,
            "unsuccessful_expectations": 0,
            "success_percent": None,
        },
        meta={
            "great_expectations_version": "v0.8.0__develop",
            "expectation_suite_name": "asset.default",
            "run_id": "test_100",
        },
    )


@pytest.fixture(scope="module")
def validation_result_suite_ge_cloud_identifier(validation_result_suite_ge_cloud_id):
    return GXCloudIdentifier(
        resource_type=GXCloudRESTResource.CHECKPOINT,
        id=validation_result_suite_ge_cloud_id,
    )


@pytest.fixture(scope="module")
def validation_result_suite_with_ge_cloud_id(validation_result_suite_ge_cloud_id):
    return ExpectationSuiteValidationResult(
        results=[],
        success=True,
        suite_name="empty_suite",
        statistics={
            "evaluated_expectations": 0,
            "successful_expectations": 0,
            "unsuccessful_expectations": 0,
            "success_percent": None,
        },
        meta={
            "great_expectations_version": "v0.8.0__develop",
            "expectation_suite_name": "asset.default",
            "run_id": "test_100",
        },
        id=validation_result_suite_ge_cloud_id,
    )


@pytest.fixture(scope="module")
def validation_result_suite_id():
    return ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier("asset.default"),
        run_id=RunIdentifier(run_name="test_100"),
        batch_identifier="1234",
    )


@pytest.fixture(scope="module")
def validation_result_suite_extended_id():
    return ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier("asset.default"),
        run_id=RunIdentifier(run_name="test_100", run_time="Tue May 08 15:14:45 +0800 2012"),
        batch_identifier=BatchIdentifier(batch_identifier="1234", data_asset_name="asset"),
    )


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def sns(aws_credentials):
    with mock_sns():
        conn = boto3.client("sns")
        yield conn
