from unittest import mock
from uuid import UUID

import pytest

import great_expectations as gx
from great_expectations.analytics.config import (
    DUMMY_UUID,
    ENV_CONFIG,
    Config,
    get_config,
    update_config,
)
from great_expectations.analytics.events import DataContextInitializedEvent
from tests.datasource.fluent._fake_cloud_api import FAKE_USER_ID

TESTING_UUID = UUID("00000000-c000-0000-0000-000000000000")


@pytest.fixture(
    scope="function",
    params=[
        (
            Config(
                organization_id=TESTING_UUID,
                user_id=TESTING_UUID,
                data_context_id=DUMMY_UUID,
                oss_id=DUMMY_UUID,
                cloud_mode=False,
            ),
            TESTING_UUID,
            {
                "user_id": TESTING_UUID,
                "organization_id": TESTING_UUID,
                "data_context_id": DUMMY_UUID,
                "oss_id": DUMMY_UUID,
                "service": "gx-core",
            },
        ),
        (
            Config(),
            DUMMY_UUID,
            {"data_context_id": DUMMY_UUID, "oss_id": DUMMY_UUID, "service": "gx-core"},
        ),
    ],
)
def analytics_config(request):
    base_config = get_config()
    update_config(request.param[0])
    yield request.param[1], request.param[2]
    update_config(base_config)


@pytest.mark.unit
def test_event_identifiers(analytics_config):
    """Validate base event properties based on the analytics config."""
    distinct_id, base_properties = analytics_config
    event = DataContextInitializedEvent()
    properties = event.properties()
    # All base properties should be in the event properties
    assert base_properties.items() <= properties.items()
    # Service should be set to gx-core
    assert properties["service"] == "gx-core"
    # The distinct_id should be the user_id if it is set, otherwise the oss_id
    assert event.distinct_id == distinct_id
    if "user_id" in base_properties:
        assert event.distinct_id == base_properties["user_id"]
    else:
        assert event.distinct_id == base_properties["oss_id"]

    # The user_id and organization_id should only be set if they are in the config
    if "user_id" not in base_properties:
        assert "user_id" not in properties
    if "organization_id" not in base_properties:
        assert "organization_id" not in properties


@pytest.mark.unit
def test_ephemeral_context_init(monkeypatch):
    monkeypatch.setattr(ENV_CONFIG, "gx_analytics_enabled", True)  # Enable usage stats

    with mock.patch(
        "great_expectations.data_context.data_context.abstract_data_context.init_analytics"
    ) as mock_init, mock.patch("posthog.capture") as mock_submit:
        _ = gx.get_context(mode="ephemeral")

    mock_init.assert_called_once_with(
        data_context_id=mock.ANY, organization_id=None, oss_id=mock.ANY, user_id=None
    )
    mock_submit.assert_called_once_with(
        mock.ANY,
        "data_context.initialized",
        {"data_context_id": mock.ANY, "oss_id": mock.ANY, "service": "gx-core"},
        groups={"data_context": mock.ANY},
    )


@pytest.mark.cloud
def test_cloud_context_init(cloud_api_fake, cloud_details, monkeypatch):
    monkeypatch.setattr(ENV_CONFIG, "gx_analytics_enabled", True)  # Enable usage stats

    with mock.patch(
        "great_expectations.data_context.data_context.cloud_data_context.init_analytics"
    ) as mock_init, mock.patch("posthog.capture") as mock_submit:
        _ = gx.get_context(
            cloud_access_token=cloud_details.access_token,
            cloud_organization_id=cloud_details.org_id,
            cloud_base_url=cloud_details.base_url,
            cloud_mode=True,
        )

    mock_init.assert_called_once_with(
        user_id=UUID(FAKE_USER_ID),  # Should be consistent with the fake Cloud API
        data_context_id=UUID(cloud_details.org_id),
        organization_id=UUID(cloud_details.org_id),
        oss_id=mock.ANY,
        cloud_mode=True,
    )
    mock_submit.assert_called_once_with(
        mock.ANY,
        "data_context.initialized",
        {"data_context_id": mock.ANY, "oss_id": mock.ANY, "service": "gx-core"},
        groups={"data_context": mock.ANY},
    )
