import pytest
try:
    from unittest import mock
except ImportError:
    import mock

from great_expectations.data_context.util import get_slack_callback, build_slack_notification_request
from .test_utils import assertDeepAlmostEqual


@pytest.fixture
def validation_json():
    return {
        "meta": {
            "data_asset_name": "diabetes_raw_csv",
            "run_id": 7,
            "result_reference": "s3://my_bucket/blah.json",
            "dataset_reference": "s3://my_bucket/blah.csv",
        },
        "statistics": {"successful_expectations": 33, "evaluated_expectations": 44},
        "success": True,
    }


def test_get_slack_callback_returns_callable():
    obs = get_slack_callback("foo")
    assert callable(obs)


def test_build_slack_notification_request_with_no_validation_json():
    with mock.patch("datetime.datetime") as mock_datetime:
        mock_datetime.strftime.return_value = "05/05/19 12:12:12"
        obs = build_slack_notification_request(None)

    assert isinstance(obs, dict)
    expected = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "No validation occurred. Please ensure you passed a validation_json.",
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Great Expectations run id None ran at 05/05/19 12:12:12",
                    }
                ],
            },
        ]
    }
    assertDeepAlmostEqual(expected, obs)


def test_build_slack_notification_request_with_successful_validation(validation_json):
    with mock.patch("datetime.datetime") as mock_datetime:
        mock_datetime.strftime.return_value = "05/05/19 12:12:12"
        obs = build_slack_notification_request(validation_json)

    assert isinstance(obs, dict)
    expected = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Validated batch from data asset:* `diabetes_raw_csv`\n*Status: Success :tada:*\n33 of 44 expectations were met\n\n",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "- *Validation Report*: s3://my_bucket/blah.json",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "- *Validation data asset*: s3://my_bucket/blah.csv",
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Great Expectations run id 7 ran at 05/05/19 12:12:12",
                    }
                ],
            },
        ]
    }
    assertDeepAlmostEqual(expected, obs)


def test_build_slack_notification_request_with_successful_validation_and_batch_kwargs(validation_json):
    batch_kwargs = {
         "path": "/Users/user/some_path/some_file.csv",
         "timestamp": "1565286704.3622668",
         "sep": None,
         "engine": "python"
    }
    validation_json["meta"]["batch_kwargs"] = batch_kwargs
    
    with mock.patch("datetime.datetime") as mock_datetime:
        mock_datetime.strftime.return_value = "05/05/19 12:12:12"
        obs = build_slack_notification_request(validation_json)

    assert isinstance(obs, dict)
    print(obs)
    assert len(obs["blocks"]) == 5
    batch_kwargs_text = obs["blocks"][1]["text"]["text"]
    for key, val in batch_kwargs.items():
        assert key in batch_kwargs_text
        if val is not None:
            assert val in batch_kwargs_text
        else:
            assert 'null' in batch_kwargs_text
    

def test_build_slack_notification_request_with_failed_validation(validation_json):
    validation_json["success"] = False
    with mock.patch("datetime.datetime") as mock_datetime:
        mock_datetime.strftime.return_value = "05/05/19 12:12:12"
        obs = build_slack_notification_request(validation_json)

    assert isinstance(obs, dict)
    expected = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Validated batch from data asset:* `diabetes_raw_csv`\n*Status: Failed :x:*\n33 of 44 expectations were met\n\n",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "- *Validation Report*: s3://my_bucket/blah.json",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "- *Validation data asset*: s3://my_bucket/blah.csv",
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Great Expectations run id 7 ran at 05/05/19 12:12:12",
                    }
                ],
            },
        ]
    }
    assertDeepAlmostEqual(expected, obs)


def test_build_slack_notification_request_with_successful_validation_and_no_result_report(
    validation_json
):
    validation_json["meta"].pop("result_reference")
    with mock.patch("datetime.datetime") as mock_datetime:
        mock_datetime.strftime.return_value = "05/05/19 12:12:12"
        obs = build_slack_notification_request(validation_json)

    assert isinstance(obs, dict)
    expected = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Validated batch from data asset:* `diabetes_raw_csv`\n*Status: Success :tada:*\n33 of 44 expectations were met\n\n",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "- *Validation data asset*: s3://my_bucket/blah.csv",
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Great Expectations run id 7 ran at 05/05/19 12:12:12",
                    }
                ],
            },
        ]
    }
    assertDeepAlmostEqual(expected, obs)


def test_build_slack_notification_request_with_successful_validation_and_no_dataset(
    validation_json
):
    validation_json["meta"].pop("dataset_reference")
    with mock.patch("datetime.datetime") as mock_datetime:
        mock_datetime.strftime.return_value = "05/05/19 12:12:12"
        obs = build_slack_notification_request(validation_json)

    assert isinstance(obs, dict)
    expected = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Validated batch from data asset:* `diabetes_raw_csv`\n*Status: Success :tada:*\n33 of 44 expectations were met\n\n",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "- *Validation Report*: s3://my_bucket/blah.json",
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Great Expectations run id 7 ran at 05/05/19 12:12:12",
                    }
                ],
            },
        ]
    }
    assertDeepAlmostEqual(expected, obs)
