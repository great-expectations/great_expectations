from __future__ import annotations

import json
import uuid
from unittest import mock

import pytest

import great_expectations as gx
from great_expectations.checkpoint.actions import (
    MicrosoftTeamsNotificationAction,
    SlackNotificationAction,
    ValidationAction,
)
from great_expectations.checkpoint.v1_checkpoint import Checkpoint
from great_expectations.core.batch_config import BatchConfig
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_config import ValidationConfig
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)


@pytest.mark.unit
def test_checkpoint_no_validations_raises_error():
    with pytest.raises(ValueError) as e:
        Checkpoint(name="my_checkpoint", validations=[], actions=[])

    assert "Checkpoint must contain at least one validation" in str(e.value)


class TestCheckpointSerialization:
    @pytest.fixture
    def in_memory_context(self) -> EphemeralDataContext:
        return gx.get_context(mode="ephemeral")

    @pytest.fixture
    def validation_config_1(
        self, in_memory_context: EphemeralDataContext, mocker: pytest.MockFixture
    ):
        name = "my_first_validation"
        vc = ValidationConfig(
            name=name,
            data=mocker.Mock(spec=BatchConfig),
            suite=mocker.Mock(spec=ExpectationSuite),
        )
        with mock.patch.object(
            ValidationConfig,
            "json",
            return_value=json.dumps({"id": str(uuid.uuid4()), "name": name}),
        ):
            yield vc

    @pytest.fixture
    def validation_config_2(
        self, in_memory_context: EphemeralDataContext, mocker: pytest.MockFixture
    ):
        name = "my_second_validation"
        vc = ValidationConfig(
            name=name,
            data=mocker.Mock(spec=BatchConfig),
            suite=mocker.Mock(spec=ExpectationSuite),
        )
        with mock.patch.object(
            ValidationConfig,
            "json",
            return_value=json.dumps({"id": str(uuid.uuid4()), "name": name}),
        ):
            yield vc

    @pytest.fixture
    def validation_configs(
        self,
        validation_config_1: ValidationConfig,
        validation_config_2: ValidationConfig,
    ) -> list[ValidationConfig]:
        return [validation_config_1, validation_config_2]

    @pytest.fixture
    def slack_action(self):
        return SlackNotificationAction(
            slack_webhook="slack_webhook",
        )

    @pytest.fixture
    def teams_action(self):
        return MicrosoftTeamsNotificationAction(
            teams_webhook="teams_webhook",
        )

    @pytest.fixture
    def actions(
        self,
        slack_action: SlackNotificationAction,
        teams_action: MicrosoftTeamsNotificationAction,
    ) -> list[ValidationAction]:
        return [slack_action, teams_action]

    @pytest.mark.parametrize(
        "action_fixture_name, expected_actions",
        [
            pytest.param(None, [], id="no_actions"),
            pytest.param(
                "actions",
                [
                    {
                        "notify_on": "all",
                        "notify_with": None,
                        "renderer": {
                            "class_name": "SlackRenderer",
                            "module_name": "great_expectations.render.renderer.slack_renderer",
                        },
                        "show_failed_expectations": False,
                        "slack_channel": None,
                        "slack_token": None,
                        "slack_webhook": "slack_webhook",
                        "type": "slack",
                    },
                    {
                        "notify_on": "all",
                        "renderer": {
                            "class_name": "MicrosoftTeamsRenderer",
                            "module_name": "great_expectations.render.renderer.microsoft_teams_renderer",
                        },
                        "teams_webhook": "teams_webhook",
                        "type": "microsoft",
                    },
                ],
                id="actions",
            ),
        ],
    )
    @pytest.mark.unit
    def test_checkpoint_serialization_success(
        self,
        validation_configs: list[ValidationConfig],
        action_fixture_name: str | None,
        expected_actions: dict,
        request: pytest.FixtureRequest,
    ):
        actions = (
            request.getfixturevalue(action_fixture_name) if action_fixture_name else []
        )
        cp = Checkpoint(
            name="my_checkpoint",
            validations=validation_configs,
            actions=actions,
        )

        actual = json.loads(cp.json(models_as_dict=False))
        expected = {
            "name": cp.name,
            "validations": [
                {
                    "id": mock.ANY,
                    "name": "my_first_validation",
                },
                {
                    "id": mock.ANY,
                    "name": "my_second_validation",
                },
            ],
            "actions": expected_actions,
            "id": cp.id,
        }

        assert actual == expected

    def test_checkpoint_serialization_failure(self):
        pass

    def test_checkpoint_deserialization_success(self):
        pass

    def test_checkpoint_deserialization_failure(self):
        pass
