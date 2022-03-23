import pytest

from great_expectations.checkpoint.util import send_sns_notification

from great_expectations.core import ExpectationSuiteValidationResult
from .conftest import sns


def test_send_sns_notification(sns):
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
    sns.create_topic({"Name": topic})
    response = send_sns_notification(topic, result.success, result.results)
    assert response.startswith("Successfully")
