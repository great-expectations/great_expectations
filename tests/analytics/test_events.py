import pytest

from great_expectations.analytics.actions import (
    EXPECTATION_SUITE_CREATED,
    EXPECTATION_SUITE_DELETED,
    EXPECTATION_SUITE_EXPECTATION_CREATED,
    EXPECTATION_SUITE_EXPECTATION_DELETED,
    EXPECTATION_SUITE_EXPECTATION_UPDATED,
)
from great_expectations.analytics.events import (
    DataContextInitializedEvent,
    ExpectationSuiteCreatedEvent,
    ExpectationSuiteDeletedEvent,
    ExpectationSuiteExpectationCreatedEvent,
    ExpectationSuiteExpectationDeletedEvent,
    ExpectationSuiteExpectationUpdatedEvent,
)


@pytest.mark.parametrize(
    "event_cls, input_kwargs, expected_properties",
    [
        pytest.param(
            DataContextInitializedEvent, {}, {}, id="DataContextInitializedEvent"
        ),
        pytest.param(
            ExpectationSuiteExpectationCreatedEvent,
            {
                "action": EXPECTATION_SUITE_EXPECTATION_CREATED,
                "expectation_id": "157abeb6-ffa8-4520-8239-649cf6ca9489",
                "expectation_suite_id": "fbb7ada0-600d-458d-a4f7-c6c30cb759b4",
                "expectation_type": "expect_column_values_to_be_between",
                "custom_exp_type": False,
            },
            {
                "expectation_id": "157abeb6-ffa8-4520-8239-649cf6ca9489",
                "expectation_suite_id": "fbb7ada0-600d-458d-a4f7-c6c30cb759b4",
                "expectation_type": "expect_column_values_to_be_between",
                "custom_exp_type": False,
            },
            id="ExpectationSuiteExpectationCreatedEvent",
        ),
        pytest.param(
            ExpectationSuiteExpectationUpdatedEvent,
            {
                "action": EXPECTATION_SUITE_EXPECTATION_UPDATED,
                "expectation_id": "157abeb6-ffa8-4520-8239-649cf6ca9489",
                "expectation_suite_id": "fbb7ada0-600d-458d-a4f7-c6c30cb759b4",
            },
            {
                "expectation_id": "157abeb6-ffa8-4520-8239-649cf6ca9489",
                "expectation_suite_id": "fbb7ada0-600d-458d-a4f7-c6c30cb759b4",
            },
            id="ExpectationSuiteExpectationUpdatedEvent",
        ),
        pytest.param(
            ExpectationSuiteExpectationDeletedEvent,
            {
                "action": EXPECTATION_SUITE_EXPECTATION_DELETED,
                "expectation_id": "157abeb6-ffa8-4520-8239-649cf6ca9489",
                "expectation_suite_id": "fbb7ada0-600d-458d-a4f7-c6c30cb759b4",
            },
            {
                "expectation_id": "157abeb6-ffa8-4520-8239-649cf6ca9489",
                "expectation_suite_id": "fbb7ada0-600d-458d-a4f7-c6c30cb759b4",
            },
            id="ExpectationSuiteExpectationDeletedEvent",
        ),
        pytest.param(
            ExpectationSuiteCreatedEvent,
            {
                "action": EXPECTATION_SUITE_CREATED,
                "expectation_suite_id": "fbb7ada0-600d-458d-a4f7-c6c30cb759b4",
            },
            {
                "expectation_suite_id": "fbb7ada0-600d-458d-a4f7-c6c30cb759b4",
            },
            id="ExpectationSuiteCreatedEvent",
        ),
        pytest.param(
            ExpectationSuiteDeletedEvent,
            {
                "action": EXPECTATION_SUITE_DELETED,
                "expectation_suite_id": "fbb7ada0-600d-458d-a4f7-c6c30cb759b4",
            },
            {
                "expectation_suite_id": "fbb7ada0-600d-458d-a4f7-c6c30cb759b4",
            },
            id="ExpectationSuiteDeletedEvent",
        ),
    ],
)
@pytest.mark.unit
def test_event_properties(event_cls, input_kwargs, expected_properties):
    event = event_cls(**input_kwargs)

    actual_properties = event.properties()

    # Assert that base properties are present
    for base_property in ("data_context_id", "organization_id", "oss_id", "service"):
        assert base_property in actual_properties
        actual_properties.pop(base_property)

    # Assert remaining event-specific properties
    assert actual_properties == expected_properties
