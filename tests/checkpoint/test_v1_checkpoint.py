from __future__ import annotations

import json
from unittest import mock
from unittest.mock import Mock

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
    def validation_config_1(self):
        vc = ValidationConfig(
            name="my_first_validation",
            data=Mock(spec=BatchConfig),
            suite=Mock(spec=ExpectationSuite),
        )

        with mock.patch.obj(ValidationConfig, "json", return_value={}):
            yield vc

    @pytest.fixture
    def validation_config_2(self):
        vc = ValidationConfig(
            name="my_second_validation",
            data=Mock(spec=BatchConfig),
            suite=Mock(spec=ExpectationSuite),
        )

        with mock.patch.obj(ValidationConfig, "json", return_value={}):
            yield vc

    @pytest.fixture
    def validation_configs(
        validation_config_1: ValidationConfig, validation_config_2: ValidationConfig
    ) -> list[ValidationConfig]:
        return [validation_config_1, validation_config_2]

    @pytest.fixture
    def slack_action(self, in_memory_context: EphemeralDataContext):
        return SlackNotificationAction(
            data_context=in_memory_context, renderer={"class_name": "SlackRenderer"}
        )

    @pytest.fixture
    def teams_action(self, in_memory_context: EphemeralDataContext):
        return MicrosoftTeamsNotificationAction(
            data_context=in_memory_context,
            renderer={"class_name": "MicrosoftTeamsRenderer"},
        )

    @pytest.fixture
    def actions(
        slack_action: SlackNotificationAction,
        teams_action: MicrosoftTeamsNotificationAction,
    ) -> list[ValidationAction]:
        return [slack_action, teams_action]

    @pytest.mark.unit
    @pytest.mark.parametrize("actions_fixture_name", ["actions", None])
    def test_checkpoint_serialization_success(
        self,
        validation_configs: list[ValidationConfig],
        actions_fixture_name: str | None,
        request: pytest.FixtureRequest,
    ):
        actions = (
            request.getfixturevalue(actions_fixture_name)
            if actions_fixture_name
            else []
        )

        cp = Checkpoint(
            name="my_checkpoint", validations=validation_configs, actions=actions
        )

        actual = json.loads(cp.json(models_as_dict=False))
        expected = {
            "name": cp.name,
            "validations": [],
            "actions": [],
            "id": cp.id,
        }

        assert actual == expected

    def test_checkpoint_serialization_failure(self):
        pass

    def test_checkpoint_deserialization_success(self):
        pass

    def test_checkpoint_deserialization_failure(self):
        pass
