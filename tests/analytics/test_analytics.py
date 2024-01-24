from uuid import UUID
import pytest
from great_expectations.analytics.config import get_config, update_config, Config, DUMMY_UUID
from great_expectations.analytics.events import DataContextInitializedEvent

TESTING_UUID = UUID("00000000-c000-0000-0000-000000000000")


@pytest.fixture(scope="function", params=[
    (Config(organization_id=TESTING_UUID, user_id=TESTING_UUID, data_context_id=DUMMY_UUID, oss_id=DUMMY_UUID, cloud_mode=False),
        TESTING_UUID,
        {"user_id": TESTING_UUID, "organization_id": TESTING_UUID, "data_context_id": DUMMY_UUID, "oss_id": DUMMY_UUID, "service": "gx-core"}),
    (Config(),
        DUMMY_UUID,
        {"data_context_id": DUMMY_UUID, "oss_id": DUMMY_UUID, "service": "gx-core"}),
])
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
