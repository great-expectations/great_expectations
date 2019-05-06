import pytest
from unittest import mock
from great_expectations import get_slack_callback, build_slack_notification_request


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
    with mock.patch("uuid.uuid4") as mock_uuid:
        mock_uuid.return_value = 99
        with mock.patch("datetime.datetime") as mock_datetime:
            mock_datetime.strftime.return_value = "05/05/19 12:12:12"
            obs = build_slack_notification_request(None)

    assert isinstance(obs, dict)
    assert obs == {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": """*Validated dataset:* `no_name_provided_99` *Status: Failed :x:*\nNo validation occurred. Please ensure you passed a validation_json.""",
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


def test_build_slack_notification_request_with_successful_validation(validation_json):
    with mock.patch("uuid.uuid4") as mock_uuid:
        mock_uuid.return_value = 99
        with mock.patch("datetime.datetime") as mock_datetime:
            mock_datetime.strftime.return_value = "05/05/19 12:12:12"
            obs = build_slack_notification_request(validation_json)

    assert isinstance(obs, dict)
    assert obs == {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": """*Validated dataset:* `diabetes_raw_csv` *Status: Success :tada:*\n33 of 44 expectations were met\n\n""",
                },
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "View Validation Report",
                        },
                        "url": validation_json["meta"]["result_reference"],
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View Dataset"},
                        "url": validation_json["meta"]["dataset_reference"],
                    },
                ],
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


def test_build_slack_notification_request_with_successful_validation_and_no_result_report(
    validation_json
):
    with mock.patch("uuid.uuid4") as mock_uuid:
        validation_json["meta"].pop("result_reference")

        mock_uuid.return_value = 99
        with mock.patch("datetime.datetime") as mock_datetime:
            mock_datetime.strftime.return_value = "05/05/19 12:12:12"
            obs = build_slack_notification_request(validation_json)

    assert isinstance(obs, dict)
    assert obs == {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": """*Validated dataset:* `diabetes_raw_csv` *Status: Success :tada:*\n33 of 44 expectations were met\n\n""",
                },
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View Dataset"},
                        "url": validation_json["meta"]["dataset_reference"],
                    }
                ],
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


def test_build_slack_notification_request_with_successful_validation_and_no_dataset(
    validation_json
):
    with mock.patch("uuid.uuid4") as mock_uuid:
        validation_json["meta"].pop("dataset_reference")

        mock_uuid.return_value = 99
        with mock.patch("datetime.datetime") as mock_datetime:
            mock_datetime.strftime.return_value = "05/05/19 12:12:12"
            obs = build_slack_notification_request(validation_json)

    assert isinstance(obs, dict)
    assert obs == {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": """*Validated dataset:* `diabetes_raw_csv` *Status: Success :tada:*\n33 of 44 expectations were met\n\n""",
                },
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "View Validation Report",
                        },
                        "url": validation_json["meta"]["result_reference"],
                    }
                ],
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
