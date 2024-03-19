from __future__ import annotations

import json
import pathlib
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
from great_expectations.compatibility.pydantic import ValidationError
from great_expectations.core.batch_config import BatchConfig
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_config import ValidationConfig
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from tests.test_utils import working_directory


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
            yield in_memory_context.validations.add(vc)

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
            yield in_memory_context.validations.add(vc)

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
    def test_checkpoint_serialization(
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

    @pytest.mark.unit
    def test_checkpoint_deserialization_success(
        self,
        in_memory_context: EphemeralDataContext,
        validation_configs: list[ValidationConfig],
        actions: list[ValidationAction],
    ):
        serialized_checkpoint = {
            "name": "my_checkpoint",
            "validations": [
                {
                    "name": validation_configs[0].name,
                    "id": validation_configs[0].id,
                },
                {
                    "name": validation_configs[1].name,
                    "id": validation_configs[1].id,
                },
            ],
            "actions": [
                {
                    "notify_on": "all",
                    "notify_with": None,
                    "renderer": {
                        "class_name": actions[0].renderer.__class__.__name__,
                        "module_name": actions[0].renderer.__class__.__module__,
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
                        "class_name": actions[1].renderer.__class__.__name__,
                        "module_name": actions[1].renderer.__class__.__module__,
                    },
                    "teams_webhook": "teams_webhook",
                    "type": "microsoft",
                },
            ],
            "id": "c758816-64c8-46cb-8f7e-03c12cea1d67",
        }

        _ = Checkpoint.parse_obj(serialized_checkpoint)

    @pytest.mark.parametrize(
        "serialized_checkpoint, expected_error",
        [
            pytest.param(
                {
                    "name": "my_checkpoint",
                    "validations": [],
                    "actions": [],
                    "id": "c758816-64c8-46cb-8f7e-03c12cea1d67",
                },
                "Checkpoint must contain at least one validation",
                id="missing_validations",
            ),
            pytest.param(
                {
                    "name": "my_checkpoint",
                    "validations": [
                        {
                            "name": "i_do_not_exist",
                            "id": "a758816-64c8-46cb-8f7e-03c12cea1d67",
                        }
                    ],
                    "actions": [],
                    "id": "c758816-64c8-46cb-8f7e-03c12cea1d67",
                },
                "Unable to retrieve validation config",
                id="nonexistent_validation",
            ),
            pytest.param(
                {
                    "name": "my_checkpoint",
                    "validations": [
                        {
                            "other_key": "i_do_not_exist",
                            "id": "a758816-64c8-46cb-8f7e-03c12cea1d67",
                        }
                    ],
                    "actions": [],
                    "id": "c758816-64c8-46cb-8f7e-03c12cea1d67",
                },
                "validations -> name\n  field required",
                id="invalid_validation",
            ),
        ],
    )
    @pytest.mark.unit
    def test_checkpoint_deserialization_failure(
        self, serialized_checkpoint: str, expected_error: str
    ):
        with pytest.raises(ValidationError) as e:
            Checkpoint.parse_obj(serialized_checkpoint)

        assert expected_error in str(e.value)

    @pytest.mark.filesystem
    def test_checkpoint_serialization_adds_ids(self, tmp_path: pathlib.Path):
        with working_directory(tmp_path):
            context = gx.get_context(mode="file")

        ds = context.sources.add_pandas("my_datasource")
        asset = ds.add_csv_asset("my_asset", "my_file.csv")

        bc1 = asset.add_batch_config("my_batch1")
        suite1 = ExpectationSuite("my_suite1")
        vc1 = ValidationConfig(name="my_validation1", data=bc1, suite=suite1)

        bc2 = asset.add_batch_config("my_batch2")
        suite2 = ExpectationSuite("my_suite2")
        vc2 = ValidationConfig(name="my_validation2", data=bc2, suite=suite2)

        validations = [vc1, vc2]
        actions = [
            SlackNotificationAction(slack_webhook="my_slack_webhook"),
            MicrosoftTeamsNotificationAction(teams_webhook="my_teams_webhook"),
        ]

        cp = Checkpoint(name="my_checkpoint", validations=validations, actions=actions)
        serialized_checkpoint = cp.json()
        serialized_checkpoint_dict = json.loads(serialized_checkpoint)

        # If not previously persisted, any nested suites and validation configs should get IDs
        validations = serialized_checkpoint_dict["validations"]
        for id in (
            validations[0]["id"],
            validations[1]["id"],
            validations[0]["suite"]["id"],
            validations[1]["suite"]["id"],
        ):
            try:
                uuid.UUID(id)
            except (ValueError, TypeError):
                pytest.fail(f"Expected {id} to be a valid UUID")
