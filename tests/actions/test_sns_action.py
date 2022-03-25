import pytest

from great_expectations.checkpoint.util import send_sns_notification
from great_expectations.core import ExpectationSuiteValidationResult

from .conftest import aws_credentials, sns


def test_send_sns_notification(sns, aws_credentials):
    results = {
        "success": True,
        "results": {
            "observed_value": 5.0,
            "element_count": 5,
            "missing_count": None,
            "missing_percent": None,
        },
    }
    result = ExpectationSuiteValidationResult(**results)
    topic = "test"
    created = sns.create_topic(Name=topic)
    response = send_sns_notification(
        created.get("TopicArn"), str(result.success), str(result.results)
    )

    assert response.startswith("Successfully")
