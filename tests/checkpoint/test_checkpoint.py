from __future__ import annotations

import json
import pathlib
import uuid
from typing import TYPE_CHECKING, List, Type
from unittest import mock

import pandas as pd
import pytest
from requests import Session

import great_expectations as gx
from great_expectations import expectations as gxe
from great_expectations.analytics.events import CheckpointRanEvent
from great_expectations.checkpoint.actions import (
    MicrosoftTeamsNotificationAction,
    OpsgenieAlertAction,
    PagerdutyAlertAction,
    SlackNotificationAction,
    UpdateDataDocsAction,
    ValidationAction,
)
from great_expectations.checkpoint.checkpoint import (
    Checkpoint,
    CheckpointAction,
    CheckpointResult,
)
from great_expectations.compatibility.pydantic import ValidationError
from great_expectations.constants import DATAFRAME_REPLACEMENT_STR
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.core.freshness_diagnostics import (
    BatchDefinitionFreshnessDiagnostics,
    CheckpointFreshnessDiagnostics,
    ExpectationSuiteFreshnessDiagnostics,
    ValidationDefinitionFreshnessDiagnostics,
)
from great_expectations.core.result_format import ResultFormat
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext
from great_expectations.data_context.data_context.context_factory import set_context
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)
from great_expectations.exceptions import (
    BatchDefinitionNotAddedError,
    CheckpointNotAddedError,
    CheckpointRelatedResourcesFreshnessError,
    CheckpointRunWithoutValidationDefinitionError,
    ExpectationSuiteNotAddedError,
    ResourceFreshnessError,
    ValidationDefinitionNotAddedError,
)
from great_expectations.exceptions.exceptions import (
    CheckpointNotFoundError,
    ValidationDefinitionNotFoundError,
)
from great_expectations.exceptions.resource_freshness import ResourceFreshnessAggregateError
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
from tests.test_utils import working_directory

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


@pytest.mark.unit
def test_checkpoint_no_validation_definitions_does_not_raise():
    Checkpoint(name="my_checkpoint", validation_definitions=[])
    # see, no errors!


@pytest.mark.unit
def test_checkpoint_save_success(mocker: MockerFixture):
    context = mocker.Mock(spec=AbstractDataContext)
    set_context(project=context)

    checkpoint = Checkpoint(
        name="my_checkpoint",
        validation_definitions=[mocker.Mock(spec=ValidationDefinition)],
    )
    store_key = context.checkpoint_store.get_key.return_value
    checkpoint.save()

    context.checkpoint_store.update.assert_called_once_with(key=store_key, value=checkpoint)


class TestValidationDefinitionInteraction:
    """
    These tests are specifically to verify the following workflow:
    * Users instantiates (but doesn't save) a ValidationDefinition
    * User instantiates (but doesn't save) a Checkpoint with the ValidationDefinition
    * User saves the ValidationDefinition
    * User saves the Checkpoint

    See https://greatexpectations.atlassian.net/browse/V1-496.
    """

    @pytest.mark.unit
    def test_checkpoint_does_not_clone_validation_definition(self):
        context = gx.get_context(mode="ephemeral")

        batch_definition = (
            context.data_sources.add_pandas("my_datasource")
            .add_dataframe_asset("my_asset")
            .add_batch_definition_whole_dataframe("my_batch_definition")
        )
        suite = context.suites.add(ExpectationSuite(name="my_suite"))
        validation_definition = ValidationDefinition(
            name="my_validation_definition", suite=suite, data=batch_definition
        )

        checkpoint = Checkpoint(
            name="my_checkpoint",
            validation_definitions=[validation_definition],
        )

        assert checkpoint.validation_definitions[0] is validation_definition

    @pytest.mark.filesystem
    def test_save_order_does_not_matter(self, tmp_path: pathlib.Path):
        with working_directory(tmp_path):
            context = gx.get_context(mode="file")

            batch_definition = (
                context.data_sources.add_pandas("my_datasource")
                .add_dataframe_asset("my_asset")
                .add_batch_definition_whole_dataframe("my_batch_definition")
            )
            suite = context.suites.add(ExpectationSuite(name="my_suite"))
            validation_definition = ValidationDefinition(
                name="my_validation_definition", suite=suite, data=batch_definition
            )

            checkpoint = Checkpoint(
                name="my_checkpoint",
                validation_definitions=[validation_definition],
            )

            context.validation_definitions.add(validation_definition)
            context.checkpoints.add(checkpoint)


@pytest.fixture
def slack_action():
    return SlackNotificationAction(
        name="my_slack_action",
        slack_webhook="${SLACK_WEBHOOK}",
    )


@pytest.fixture
def teams_action():
    return MicrosoftTeamsNotificationAction(
        name="my_teams_action",
        teams_webhook="teams_webhook",
    )


@pytest.fixture
def actions(
    slack_action: SlackNotificationAction,
    teams_action: MicrosoftTeamsNotificationAction,
) -> list[ValidationAction]:
    return [slack_action, teams_action]


class TestCheckpointSerialization:
    @pytest.fixture
    def in_memory_context(self) -> EphemeralDataContext:
        return gx.get_context(mode="ephemeral")

    @pytest.fixture
    def validation_definition_1(
        self, in_memory_context: EphemeralDataContext, mocker: MockerFixture
    ):
        name = "my_first_validation"
        vc = ValidationDefinition(
            name=name,
            data=mocker.Mock(spec=BatchDefinition),
            suite=mocker.Mock(spec=ExpectationSuite),
        )
        with (
            mock.patch.object(
                ValidationDefinition,
                "json",
                return_value=json.dumps({"id": str(uuid.uuid4()), "name": name}),
            ),
            mock.patch.object(
                ValidationDefinition,
                "is_fresh",
                return_value=ValidationDefinitionFreshnessDiagnostics(errors=[]),
            ),
        ):
            yield in_memory_context.validation_definitions.add(vc)

    @pytest.fixture
    def validation_definition_2(
        self, in_memory_context: EphemeralDataContext, mocker: MockerFixture
    ):
        name = "my_second_validation"
        vc = ValidationDefinition(
            name=name,
            data=mocker.Mock(spec=BatchDefinition),
            suite=mocker.Mock(spec=ExpectationSuite),
        )
        with (
            mock.patch.object(
                ValidationDefinition,
                "json",
                return_value=json.dumps({"id": str(uuid.uuid4()), "name": name}),
            ),
            mock.patch.object(
                ValidationDefinition,
                "is_fresh",
                return_value=ValidationDefinitionFreshnessDiagnostics(errors=[]),
            ),
        ):
            yield in_memory_context.validation_definitions.add(vc)

    @pytest.fixture
    def validation_definitions(
        self,
        validation_definition_1: ValidationDefinition,
        validation_definition_2: ValidationDefinition,
    ) -> list[ValidationDefinition]:
        return [validation_definition_1, validation_definition_2]

    @pytest.mark.parametrize(
        "action_fixture_name, expected_actions",
        [
            pytest.param(None, [], id="no_actions"),
            pytest.param(
                "actions",
                [
                    {
                        "name": "my_slack_action",
                        "notify_on": "all",
                        "notify_with": None,
                        "renderer": {
                            "class_name": "SlackRenderer",
                            "module_name": "great_expectations.render.renderer.slack_renderer",
                        },
                        "show_failed_expectations": False,
                        "slack_channel": None,
                        "slack_token": None,
                        "slack_webhook": "${SLACK_WEBHOOK}",
                        "type": "slack",
                    },
                    {
                        "name": "my_teams_action",
                        "notify_on": "all",
                        "renderer": {
                            "class_name": "MicrosoftTeamsRenderer",
                            "module_name": "great_expectations.render.renderer.microsoft_teams_renderer",  # noqa: E501
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
        validation_definitions: list[ValidationDefinition],
        action_fixture_name: str | None,
        expected_actions: dict,
        request: pytest.FixtureRequest,
    ):
        actions = request.getfixturevalue(action_fixture_name) if action_fixture_name else []
        cp = Checkpoint(
            name="my_checkpoint",
            validation_definitions=validation_definitions,
            actions=actions,
        )

        actual = json.loads(cp.json(models_as_dict=False))
        expected = {
            "name": cp.name,
            "validation_definitions": [
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
            "result_format": ResultFormat.SUMMARY,
            "id": cp.id,
        }

        assert actual == expected

        # Validation definitions should be persisted and obtain IDs before serialization
        self._assert_valid_uuid(actual["validation_definitions"][0]["id"])
        self._assert_valid_uuid(actual["validation_definitions"][1]["id"])

    @pytest.mark.filesystem
    def test_checkpoint_filesystem_round_trip_adds_ids(
        self,
        tmp_path: pathlib.Path,
        actions: list[CheckpointAction],
    ):
        with working_directory(tmp_path):
            context = gx.get_context(mode="file")

        ds_name = "my_datasource"
        asset_name = "my_asset"
        batch_definition_name_1 = "my_batch1"
        suite_name_1 = "my_suite1"
        validation_definition_name_1 = "my_validation1"
        batch_definition_name_2 = "my_batch2"
        suite_name_2 = "my_suite2"
        validation_definition_name_2 = "my_validation2"
        cp_name = "my_checkpoint"

        ds = context.data_sources.add_pandas(ds_name)
        asset = ds.add_csv_asset(asset_name, "my_file.csv")  # type: ignore[arg-type]

        bc1 = asset.add_batch_definition(batch_definition_name_1)
        suite1 = context.suites.add(ExpectationSuite(suite_name_1))
        vc1 = context.validation_definitions.add(
            ValidationDefinition(name=validation_definition_name_1, data=bc1, suite=suite1)
        )

        bc2 = asset.add_batch_definition(batch_definition_name_2)
        suite2 = context.suites.add(ExpectationSuite(suite_name_2))
        vc2 = context.validation_definitions.add(
            ValidationDefinition(name=validation_definition_name_2, data=bc2, suite=suite2)
        )

        validation_definitions = [vc1, vc2]
        cp = Checkpoint(
            name=cp_name, validation_definitions=validation_definitions, actions=actions
        )

        serialized_checkpoint = cp.json(models_as_dict=False)
        serialized_checkpoint_dict = json.loads(serialized_checkpoint)

        assert serialized_checkpoint_dict == {
            "name": cp_name,
            "validation_definitions": [
                {
                    "id": mock.ANY,
                    "name": validation_definition_name_1,
                },
                {
                    "id": mock.ANY,
                    "name": validation_definition_name_2,
                },
            ],
            "actions": [
                {
                    "name": "my_slack_action",
                    "notify_on": "all",
                    "notify_with": None,
                    "renderer": {
                        "class_name": "SlackRenderer",
                        "module_name": "great_expectations.render.renderer.slack_renderer",
                    },
                    "show_failed_expectations": False,
                    "slack_channel": None,
                    "slack_token": None,
                    "slack_webhook": "${SLACK_WEBHOOK}",
                    "type": "slack",
                },
                {
                    "name": "my_teams_action",
                    "notify_on": "all",
                    "renderer": {
                        "class_name": "MicrosoftTeamsRenderer",
                        "module_name": "great_expectations.render.renderer.microsoft_teams_renderer",  # noqa: E501
                    },
                    "teams_webhook": "teams_webhook",
                    "type": "microsoft",
                },
            ],
            "result_format": ResultFormat.SUMMARY,
            "id": None,
        }

        cp = Checkpoint.parse_raw(serialized_checkpoint)

        # Check that all nested objects have been built properly with their appropriate names
        assert cp.name == cp_name
        assert cp.validation_definitions[0].data_source.name == ds_name
        assert cp.validation_definitions[0].asset.name == asset_name

        assert cp.validation_definitions[0].name == validation_definition_name_1
        assert cp.validation_definitions[0].batch_definition.name == batch_definition_name_1
        assert cp.validation_definitions[0].suite.name == suite_name_1

        assert cp.validation_definitions[1].name == validation_definition_name_2
        assert cp.validation_definitions[1].batch_definition.name == batch_definition_name_2
        assert cp.validation_definitions[1].suite.name == suite_name_2

        # Check that all validation_definitions and nested suites have been assigned IDs during serialization  # noqa: E501
        self._assert_valid_uuid(id=cp.validation_definitions[0].id)
        self._assert_valid_uuid(id=cp.validation_definitions[1].id)
        self._assert_valid_uuid(id=cp.validation_definitions[0].suite.id)
        self._assert_valid_uuid(id=cp.validation_definitions[1].suite.id)

    def _assert_valid_uuid(self, id: str | None) -> None:
        if not id:
            pytest.fail("id is None when it should be a valid UUID generated from a Store.")

        try:
            uuid.UUID(id)
        except ValueError:
            pytest.fail(f"{id} is not a valid UUID.")

    @pytest.mark.parametrize(
        "serialized_checkpoint, expected_error",
        [
            pytest.param(
                {
                    "name": "my_checkpoint",
                    "validation_definitions": [
                        {
                            "name": "i_do_not_exist",
                            "id": "a758816-64c8-46cb-8f7e-03c12cea1d67",
                        }
                    ],
                    "actions": [],
                    "id": "c758816-64c8-46cb-8f7e-03c12cea1d67",
                },
                "Unable to retrieve validation definition",
                id="nonexistent_validation",
            ),
            pytest.param(
                {
                    "name": "my_checkpoint",
                    "validation_definitions": [
                        {
                            "other_key": "i_do_not_exist",
                            "id": "a758816-64c8-46cb-8f7e-03c12cea1d67",
                        }
                    ],
                    "actions": [],
                    "id": "c758816-64c8-46cb-8f7e-03c12cea1d67",
                },
                "validation_definitions -> name\n  field required",
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

    @pytest.mark.unit
    def test_checkpoint_deserialization_with_actions(self, mocker: MockerFixture):
        # Arrange
        context = mocker.Mock(spec=AbstractDataContext)
        context.validation_definition_store.get.return_value = mocker.Mock(
            spec=ValidationDefinition
        )
        set_context(context)

        # Act
        serialized_checkpoint = {
            "actions": [
                {"name": "my_docs_action", "site_names": [], "type": "update_data_docs"},
                {"name": "my_slack_action", "slack_webhook": "test", "type": "slack"},
                {"name": "my_teams_action", "teams_webhook": "test", "type": "microsoft"},
            ],
            "id": "e7d1f462-821b-429c-8086-cca80eeea5e9",
            "name": "my_checkpoint",
            "result_format": "SUMMARY",
            "validation_definitions": [
                {"id": "3fb9ce09-a8fb-44d6-8abd-7d699443f6a1", "name": "my_validation_def"}
            ],
        }
        checkpoint = Checkpoint.parse_obj(serialized_checkpoint)

        # Assert
        assert len(checkpoint.actions) == 3
        assert isinstance(checkpoint.actions[0], UpdateDataDocsAction)
        assert isinstance(checkpoint.actions[1], SlackNotificationAction)
        assert isinstance(checkpoint.actions[2], MicrosoftTeamsNotificationAction)


class TestCheckpointResult:
    suite_name: str = "my_suite"
    column_name = "passenger_count"
    datasource_name: str = "my_pandas_datasource"
    asset_name: str = "my_asset"
    batch_definition_name: str = "my_batch_def"
    validation_definition_name: str = "my_validation_def"
    checkpoint_name: str = "my_checkpoint"

    @pytest.fixture
    def mock_suite(self, mocker: MockerFixture):
        suite = mocker.Mock(spec=ExpectationSuite)
        suite.name = self.suite_name
        suite.is_fresh.return_value = ExpectationSuiteFreshnessDiagnostics(errors=[])
        return suite

    @pytest.fixture
    def mock_batch_def(self, mocker: MockerFixture):
        bd = mocker.Mock(spec=BatchDefinition)
        bd._copy_and_set_values().is_fresh.return_value = BatchDefinitionFreshnessDiagnostics(
            errors=[]
        )
        return bd

    @pytest.fixture
    def validation_definition(self, mock_suite: MockerFixture, mock_batch_def: MockerFixture):
        validation_definition = ValidationDefinition(
            name=self.validation_definition_name,
            id=str(uuid.uuid4()),
            data=mock_batch_def,
            suite=mock_suite,
        )

        mock_run_result = ExpectationSuiteValidationResult(
            success=True,
            results=[
                ExpectationValidationResult(
                    success=True,
                    expectation_config=ExpectationConfiguration(
                        type="expect_column_values_to_be_between",
                        kwargs={
                            "batch_id": f"{self.datasource_name}-{self.asset_name}",
                            "column": self.column_name,
                            "min_value": 0.0,
                            "max_value": 100.0,
                        },
                    ),
                    result={
                        "element_count": 100000,
                        "unexpected_count": 0,
                        "unexpected_percent": 0.0,
                        "partial_unexpected_list": [],
                        "missing_count": 0,
                        "missing_percent": 0.0,
                        "unexpected_percent_total": 0.0,
                        "unexpected_percent_nonmissing": 0.0,
                        "partial_unexpected_counts": [],
                        "partial_unexpected_index_list": [],
                    },
                ),
            ],
            suite_name=self.suite_name,
            statistics={
                "evaluated_expectations": 1,
                "successful_expectations": 1,
                "unsuccessful_expectations": 0,
                "success_percent": 100.0,
            },
            batch_id=f"{self.datasource_name}-{self.asset_name}",
        )

        with mock.patch.object(ValidationDefinition, "run", return_value=mock_run_result):
            yield validation_definition

    @pytest.mark.unit
    def test_checkpoint_run_no_validation_definitions(self, mocker):
        context = mocker.Mock(spec=AbstractDataContext)
        set_context(project=context)
        checkpoint = Checkpoint(name=self.checkpoint_name, validation_definitions=[])

        with pytest.raises(CheckpointRunWithoutValidationDefinitionError):
            checkpoint.run()

    @pytest.mark.unit
    def test_checkpoint_run_dependencies_not_added_raises_error(
        self, validation_definition: ValidationDefinition
    ):
        validation_definition.id = None
        checkpoint = Checkpoint(
            name=self.checkpoint_name, validation_definitions=[validation_definition]
        )

        with pytest.raises(CheckpointRelatedResourcesFreshnessError) as e:
            checkpoint.run()

        assert [type(err) for err in e.value.errors] == [
            ValidationDefinitionNotAddedError,
            CheckpointNotAddedError,
        ]

    @pytest.mark.unit
    def test_checkpoint_run_no_actions(
        self, validation_definition: ValidationDefinition, mocker: MockerFixture
    ):
        context = mocker.Mock(spec=AbstractDataContext)
        set_context(project=context)
        checkpoint = Checkpoint(
            name=self.checkpoint_name, validation_definitions=[validation_definition]
        )

        result = checkpoint.run()

        run_results = result.run_results
        assert len(run_results) == len(checkpoint.validation_definitions)

        validation_result = tuple(run_results.values())[0]
        assert validation_result.success is True
        assert len(validation_result.results) == 1 and validation_result.results[0].success is True

        assert result.success is True

    @pytest.mark.unit
    def test_checkpoint_run_actions(
        self,
        validation_definition: ValidationDefinition,
        mocker: MockerFixture,
    ):
        # Arrange
        action = mocker.Mock(spec=UpdateDataDocsAction, type="update_data_docs")
        checkpoint = Checkpoint(
            name=self.checkpoint_name,
            validation_definitions=[validation_definition],
            actions=[action],
        )

        # Act
        result = checkpoint.run()

        # Assert
        action._copy_and_set_values().run.assert_called_once_with(
            checkpoint_result=result, action_context=mock.ANY
        )

    @pytest.mark.unit
    def test_checkpoint_sorts_actions(self, validation_definition: ValidationDefinition):
        """
        Note that we are directly testing the `_sort_actions()` private method here.

        This was done as a way to expedite the testing process due to some conflicts with
        Pydantics and mocks.
        Ideally, this would be tested through the public `run()` method.
        """
        pd_action = PagerdutyAlertAction(
            name="my_pagerduty_action", api_key="api_key", routing_key="routing_key"
        )
        og_action = OpsgenieAlertAction(name="my_opsgenie_action", api_key="api_key")
        data_docs_action = UpdateDataDocsAction(name="my_docs_action")
        actions: List[CheckpointAction] = [pd_action, og_action, data_docs_action]

        validation_definitions = [validation_definition]
        checkpoint = Checkpoint(
            name=self.checkpoint_name,
            validation_definitions=validation_definitions,
            actions=actions,
        )

        assert checkpoint._sort_actions() == [data_docs_action, pd_action, og_action]

    @pytest.mark.unit
    def test_checkpoint_run_passes_through_runtime_params(
        self, validation_definition: ValidationDefinition
    ):
        checkpoint_id = str(uuid.uuid4())
        checkpoint = Checkpoint(
            id=checkpoint_id,
            name=self.checkpoint_name,
            validation_definitions=[validation_definition],
        )
        batch_parameters = {"my_param": "my_value"}
        expectation_parameters = {"my_other_param": "my_other_value"}

        with mock.patch.object(
            Checkpoint, "is_fresh", return_value=CheckpointFreshnessDiagnostics(errors=[])
        ):
            _ = checkpoint.run(
                batch_parameters=batch_parameters, expectation_parameters=expectation_parameters
            )

        validation_definition.run.assert_called_with(  # type: ignore[attr-defined]
            checkpoint_id=checkpoint_id,
            batch_parameters=batch_parameters,
            expectation_parameters=expectation_parameters,
            result_format=ResultFormat.SUMMARY,
            run_id=mock.ANY,
        )

    @pytest.mark.unit
    def test_checkpoint_run_sends_analytics(
        self, validation_definition: ValidationDefinition, mocker: MockerFixture
    ):
        # Arrange
        submit_analytics_event = mocker.patch(
            "great_expectations.checkpoint.checkpoint.submit_analytics_event"
        )

        context = mocker.Mock(spec=AbstractDataContext)
        set_context(project=context)
        checkpoint_id = "e051d0a8-d492-4cde-91f3-29f353758488"
        checkpoint = Checkpoint(
            id=checkpoint_id,
            name=self.checkpoint_name,
            validation_definitions=[validation_definition],
        )

        # Act
        with mock.patch.object(
            Checkpoint, "is_fresh", return_value=CheckpointFreshnessDiagnostics(errors=[])
        ):
            checkpoint.run()

        # Assert
        submit_analytics_event.assert_called_once_with(
            event=CheckpointRanEvent(
                checkpoint_id=checkpoint_id, validation_definition_ids=[validation_definition.id]
            )
        )

    @pytest.mark.unit
    def test_result_init_no_run_results_raises_error(self, mocker: MockerFixture):
        with pytest.raises(ValueError) as e:
            CheckpointResult(
                run_id=mocker.Mock(spec=RunIdentifier),
                run_results={},
                checkpoint_config=mocker.Mock(spec=Checkpoint),
            )

        assert "CheckpointResult must contain at least one run result" in str(e.value)

    @pytest.mark.unit
    def test_result_describe(self, mocker: MockerFixture):
        result = CheckpointResult(
            run_id=mocker.Mock(spec=RunIdentifier),
            run_results={
                mocker.Mock(spec=ValidationResultIdentifier): mocker.Mock(
                    spec=ExpectationSuiteValidationResult, success=True
                ),
            },
            checkpoint_config=mocker.Mock(spec=Checkpoint),
        )

        with mock.patch.object(
            ExpectationSuiteValidationResult,
            "describe_dict",
            return_value={
                "success": True,
                "statistics": {
                    "evaluated_expectations": 1,
                    "successful_expectations": 1,
                    "unsuccessful_expectations": 0,
                    "success_percent": 100.0,
                },
                "expectations": [
                    {
                        "expectation_type": "expect_column_values_to_be_between",
                        "success": True,
                        "kwargs": {
                            "batch_id": "default_pandas_datasource-#ephemeral_pandas_asset",
                            "mostly": 0.95,
                            "column": "passenger_count",
                            "min_value": 0.0,
                            "max_value": 6.0,
                        },
                        "result": {
                            "element_count": 100000,
                            "unexpected_count": 1,
                            "unexpected_percent": 0.001,
                            "partial_unexpected_list": [7.0],
                            "unexpected_percent_total": 0.001,
                            "unexpected_percent_nonmissing": 0.001,
                            "partial_unexpected_counts": [{"value": 7.0, "count": 1}],
                            "partial_unexpected_index_list": [48422],
                        },
                    },
                ],
            },
            batch_id="default_pandas_datasource-#ephemeral_pandas_asset",
        ):
            actual = result.describe_dict()

        validation_results = actual.pop("validation_results")  # type: ignore[misc] # key is required
        assert len(validation_results) == 1

        expected = {
            "success": True,
            "statistics": {
                "evaluated_validations": 1,
                "success_percent": 100.0,
                "successful_validations": 1,
                "unsuccessful_validations": 0,
            },
        }
        assert actual == expected

    def _build_file_backed_checkpoint(
        self, tmp_path: pathlib.Path, actions: list[CheckpointAction] | None = None
    ) -> Checkpoint:
        actions = actions or []
        with working_directory(tmp_path):
            context = gx.get_context(mode="file")

        ds = context.data_sources.add_pandas(self.datasource_name)
        csv_path = (
            pathlib.Path(__file__).parent.parent
            / "test_sets"
            / "quickstart"
            / "yellow_tripdata_sample_2022-01.csv"
        )
        assert csv_path.exists()
        asset = ds.add_csv_asset(self.asset_name, filepath_or_buffer=csv_path)

        batch_definition = asset.add_batch_definition(self.batch_definition_name)
        suite = context.suites.add(
            ExpectationSuite(
                name=self.suite_name,
                expectations=[
                    gxe.ExpectColumnValuesToBeBetween(
                        column=self.column_name, min_value=0, max_value=10
                    ),
                    gxe.ExpectColumnMeanToBeBetween(
                        column=self.column_name,
                        min_value=0,
                        max_value=1,
                    ),
                ],
            )
        )

        validation_definition = context.validation_definitions.add(
            ValidationDefinition(
                name=self.validation_definition_name, data=batch_definition, suite=suite
            )
        )

        return Checkpoint(
            name=self.checkpoint_name,
            validation_definitions=[validation_definition],
            actions=actions,
        )

    @pytest.mark.unit
    def test_checkpoint_result_does_not_contain_dataframe(
        self,
        tmp_path: pathlib.Path,
    ):
        df = pd.DataFrame({"passenger_count": [1, 2, 3, 4, 5]})

        context = gx.get_context(mode="ephemeral")
        ds = context.data_sources.add_pandas(self.datasource_name)
        asset = ds.add_dataframe_asset(name=self.asset_name)
        batch_definition = asset.add_batch_definition_whole_dataframe(self.batch_definition_name)

        suites = context.suites.add(
            ExpectationSuite(
                name=self.suite_name,
                expectations=[
                    gxe.ExpectColumnValuesToBeBetween(
                        column=self.column_name, min_value=0, max_value=6
                    )
                ],
            )
        )
        validation_definition = context.validation_definitions.add(
            ValidationDefinition(
                name=self.validation_definition_name, data=batch_definition, suite=suites
            )
        )
        checkpoint = Checkpoint(
            name=self.checkpoint_name,
            validation_definitions=[validation_definition],
            result_format="COMPLETE",
        )
        results = checkpoint.run(batch_parameters={"dataframe": df})
        assert len(results.run_results) == 1
        result = list(results.run_results.values())[0]
        assert result.meta["batch_parameters"]["dataframe"] == DATAFRAME_REPLACEMENT_STR
        assert (
            result.meta["active_batch_definition"]["batch_identifiers"]["dataframe"]
            == DATAFRAME_REPLACEMENT_STR
        )

    @pytest.mark.filesystem
    def test_checkpoint_run_result_shape(self, tmp_path: pathlib.Path):
        checkpoint = self._build_file_backed_checkpoint(tmp_path)

        result = checkpoint.run()

        assert result.success is False
        assert result.describe_dict() == {
            "success": False,
            "statistics": {
                "evaluated_validations": 1,
                "success_percent": 0.0,
                "successful_validations": 0,
                "unsuccessful_validations": 1,
            },
            "validation_results": [
                {
                    "expectations": [
                        {
                            "expectation_type": "expect_column_values_to_be_between",
                            "kwargs": {
                                "batch_id": f"{self.datasource_name}-{self.asset_name}",
                                "column": self.column_name,
                                "max_value": 10.0,
                                "min_value": 0.0,
                            },
                            "result": {
                                "element_count": 100000,
                                "missing_count": 0,
                                "missing_percent": 0.0,
                                "partial_unexpected_counts": [],
                                "partial_unexpected_index_list": [],
                                "partial_unexpected_list": [],
                                "unexpected_count": 0,
                                "unexpected_percent": 0.0,
                                "unexpected_percent_nonmissing": 0.0,
                                "unexpected_percent_total": 0.0,
                            },
                            "success": True,
                        },
                        {
                            "expectation_type": "expect_column_mean_to_be_between",
                            "kwargs": {
                                "batch_id": f"{self.datasource_name}-{self.asset_name}",
                                "column": self.column_name,
                                "max_value": 1.0,
                                "min_value": 0.0,
                            },
                            "result": {"observed_value": 1.54471},
                            "success": False,
                        },
                    ],
                    "statistics": {
                        "evaluated_expectations": 2,
                        "success_percent": 50.0,
                        "successful_expectations": 1,
                        "unsuccessful_expectations": 1,
                    },
                    "success": False,
                    "result_url": None,
                },
            ],
        }

    @pytest.mark.filesystem
    def test_checkpoint_run_adds_requisite_ids(
        self,
        tmp_path: pathlib.Path,
    ):
        checkpoint = self._build_file_backed_checkpoint(tmp_path)

        # A checkpoint that has not been persisted before running
        # should be saved during the run
        # This is also true for its nested validation definitions
        assert checkpoint.id is None

        result = checkpoint.run()

        assert checkpoint.id is not None

        # The meta attribute of each run result should contain the requisite IDs
        run_results = tuple(result.run_results.values())
        meta = run_results[0].meta
        assert sorted(meta.keys()) == [
            "active_batch_definition",
            "batch_markers",
            "batch_parameters",
            "batch_spec",
            "checkpoint_id",
            "great_expectations_version",
            "run_id",
            "validation_id",
            "validation_time",
        ]
        assert meta["checkpoint_id"] == checkpoint.id
        assert meta["validation_id"] == checkpoint.validation_definitions[0].id

    @pytest.mark.filesystem
    def test_checkpoint_run_with_data_docs_and_slack_actions_emit_page_links(
        self,
        tmp_path: pathlib.Path,
    ):
        actions = [
            SlackNotificationAction(name="slack_action", slack_webhook="webhook"),
            UpdateDataDocsAction(name="docs_action"),
        ]
        checkpoint = self._build_file_backed_checkpoint(tmp_path=tmp_path, actions=actions)

        with mock.patch.object(Session, "post") as mock_post:
            _ = checkpoint.run()

        mock_post.assert_called_once()
        docs_block = mock_post.call_args.kwargs["json"]["blocks"][3]["text"]["text"]
        assert "*DataDocs*" in docs_block
        assert "local_site" in docs_block
        assert "file:///" in docs_block

    @pytest.mark.filesystem
    def test_checkpoint_run_with_slack_action_no_page_links(
        self,
        tmp_path: pathlib.Path,
    ):
        actions = [
            SlackNotificationAction(name="slack_action", slack_webhook="webhook"),
        ]
        checkpoint = self._build_file_backed_checkpoint(tmp_path=tmp_path, actions=actions)

        with mock.patch.object(Session, "post") as mock_post:
            _ = checkpoint.run()

        mock_post.assert_called_once()
        blocks = mock_post.call_args.kwargs["json"]["blocks"]
        # No DataDocs blocks should be present
        assert blocks == [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "slack_action - my_checkpoint - Failure :no_entry:",
                },
            },
            {"type": "section", "text": {"type": "plain_text", "text": mock.ANY}},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Asset*: my_asset  *Expectation Suite*: my_suite",
                },
            },
            {"type": "divider"},
        ]


@pytest.mark.parametrize(
    "has_id,has_validation_def_id,has_suite_id,has_batch_def_id,error_list",
    [
        pytest.param(
            True,
            True,
            True,
            True,
            [],
            id="checkpoint_id|validation_id|suite_id|batch_def_id",
        ),
        pytest.param(
            True,
            True,
            True,
            False,
            [BatchDefinitionNotAddedError],
            id="checkpoint_id|validation_id|suite_id|no_batch_def_id",
        ),
        pytest.param(
            True,
            True,
            False,
            True,
            [ExpectationSuiteNotAddedError],
            id="checkpoint_id|validation_id|no_suite_id|batch_def_id",
        ),
        pytest.param(
            True,
            True,
            False,
            False,
            [BatchDefinitionNotAddedError, ExpectationSuiteNotAddedError],
            id="checkpoint_id|validation_id|no_suite_id|no_batch_def_id",
        ),
        pytest.param(
            True,
            False,
            True,
            True,
            [ValidationDefinitionNotAddedError],
            id="checkpoint_id|no_validation_id|suite_id|batch_def_id",
        ),
        pytest.param(
            True,
            False,
            True,
            False,
            [BatchDefinitionNotAddedError, ValidationDefinitionNotAddedError],
            id="checkpoint_id|no_validation_id|suite_id|no_batch_def_id",
        ),
        pytest.param(
            True,
            False,
            False,
            True,
            [ExpectationSuiteNotAddedError, ValidationDefinitionNotAddedError],
            id="checkpoint_id|no_validation_id|no_suite_id|batch_def_id",
        ),
        pytest.param(
            True,
            False,
            False,
            False,
            [
                BatchDefinitionNotAddedError,
                ExpectationSuiteNotAddedError,
                ValidationDefinitionNotAddedError,
            ],
            id="checkpoint_id|no_validation_id|no_suite_id|no_batch_def_id",
        ),
        pytest.param(
            False,
            True,
            True,
            True,
            [CheckpointNotAddedError],
            id="no_checkpoint_id|validation_id|suite_id|batch_def_id",
        ),
        pytest.param(
            False,
            True,
            True,
            False,
            [BatchDefinitionNotAddedError, CheckpointNotAddedError],
            id="no_checkpoint_id|validation_id|suite_id|no_batch_def_id",
        ),
        pytest.param(
            False,
            True,
            False,
            True,
            [ExpectationSuiteNotAddedError, CheckpointNotAddedError],
            id="no_checkpoint_id|validation_id|no_suite_id|batch_def_id",
        ),
        pytest.param(
            False,
            True,
            False,
            False,
            [BatchDefinitionNotAddedError, ExpectationSuiteNotAddedError, CheckpointNotAddedError],
            id="no_checkpoint_id|validation_id|no_suite_id|no_batch_def_id",
        ),
        pytest.param(
            False,
            False,
            True,
            True,
            [ValidationDefinitionNotAddedError, CheckpointNotAddedError],
            id="no_checkpoint_id|no_validation_id|suite_id|batch_def_id",
        ),
        pytest.param(
            False,
            False,
            True,
            False,
            [
                BatchDefinitionNotAddedError,
                ValidationDefinitionNotAddedError,
                CheckpointNotAddedError,
            ],
            id="no_checkpoint_id|no_validation_id|suite_id|no_batch_def_id",
        ),
        pytest.param(
            False,
            False,
            False,
            True,
            [
                ExpectationSuiteNotAddedError,
                ValidationDefinitionNotAddedError,
                CheckpointNotAddedError,
            ],
            id="no_checkpoint_id|no_validation_id|no_suite_id|batch_def_id",
        ),
        pytest.param(
            False,
            False,
            False,
            False,
            [
                BatchDefinitionNotAddedError,
                ExpectationSuiteNotAddedError,
                ValidationDefinitionNotAddedError,
                CheckpointNotAddedError,
            ],
            id="no_checkpoint_id|no_validation_id|no_suite_id|no_batch_def_id",
        ),
    ],
)
@pytest.mark.unit
def test_is_fresh(
    in_memory_runtime_context,
    has_id: bool,
    has_validation_def_id: bool,
    has_suite_id: bool,
    has_batch_def_id: bool,
    error_list: list[Type[ResourceFreshnessError]],
):
    context = in_memory_runtime_context

    batch_definition = (
        context.data_sources.add_pandas(name="my_pandas_ds")
        .add_csv_asset(name="my_csv_asset", filepath_or_buffer="data.csv")
        .add_batch_definition(name="my_batch_def")
    )

    suite = context.suites.add(ExpectationSuite(name="my_suite"))

    validation_definition = context.validation_definitions.add(
        ValidationDefinition(
            name="my_validation_definition",
            suite=suite,
            data=batch_definition,
        )
    )

    checkpoint = context.checkpoints.add(
        Checkpoint(
            name="my_checkpoint",
            validation_definitions=[validation_definition],
        )
    )

    # Stores/Fluent API will always assign IDs but we manually override them here
    # for purposes of changing object state for the test
    if not has_batch_def_id:
        checkpoint.validation_definitions[0].data.id = None
    if not has_suite_id:
        checkpoint.validation_definitions[0].suite.id = None
    if not has_validation_def_id:
        checkpoint.validation_definitions[0].id = None
    if not has_id:
        checkpoint.id = None

    diagnostics = checkpoint.is_fresh()
    try:
        diagnostics.raise_for_error()
    except ResourceFreshnessAggregateError as e:
        assert [type(err) for err in e.errors] == error_list


@pytest.mark.unit
def test_is_fresh_raises_error_when_checkpoint_not_found(in_memory_runtime_context):
    context = in_memory_runtime_context

    batch_definition = (
        context.data_sources.add_pandas(name="my_pandas_ds")
        .add_csv_asset(name="my_csv_asset", filepath_or_buffer="data.csv")
        .add_batch_definition(name="my_batch_def")
    )

    suite = context.suites.add(ExpectationSuite(name="my_suite"))

    validation_definition = context.validation_definitions.add(
        ValidationDefinition(
            name="my_validation_definition",
            suite=suite,
            data=batch_definition,
        )
    )

    checkpoint = context.checkpoints.add(
        Checkpoint(
            name="my_checkpoint",
            validation_definitions=[validation_definition],
        )
    )

    context.checkpoints.delete(checkpoint.name)

    diagnostics = checkpoint.is_fresh()
    assert diagnostics.success is False
    assert len(diagnostics.errors) == 1
    assert isinstance(diagnostics.errors[0], CheckpointNotFoundError)


@pytest.mark.unit
def test_is_fresh_raises_error_when_child_deps_not_found(in_memory_runtime_context):
    context = in_memory_runtime_context

    batch_definition = (
        context.data_sources.add_pandas(name="my_pandas_ds")
        .add_csv_asset(name="my_csv_asset", filepath_or_buffer="data.csv")
        .add_batch_definition(name="my_batch_def")
    )

    suite = context.suites.add(ExpectationSuite(name="my_suite"))

    validation_definition = context.validation_definitions.add(
        ValidationDefinition(
            name="my_validation_definition",
            suite=suite,
            data=batch_definition,
        )
    )

    checkpoint = context.checkpoints.add(
        Checkpoint(
            name="my_checkpoint",
            validation_definitions=[validation_definition],
        )
    )

    context.validation_definitions.delete(validation_definition.name)

    diagnostics = checkpoint.is_fresh()
    assert diagnostics.success is False
    assert len(diagnostics.errors) == 1
    assert isinstance(diagnostics.errors[0], ValidationDefinitionNotFoundError)


@pytest.mark.unit
def test_is_fresh_raises_errors_for_all_child_validation_definitions(in_memory_runtime_context):
    context = in_memory_runtime_context

    datasource = context.data_sources.add_pandas(name="my_pandas_ds")
    asset = datasource.add_dataframe_asset(name="my_pandas_asset")
    bd_1 = asset.add_batch_definition_whole_dataframe(name="my_bd_1")
    bd_2 = asset.add_batch_definition_whole_dataframe(name="my_bd_2")

    suite_1 = ExpectationSuite(name="my_suite_1")
    suite_2 = ExpectationSuite(name="my_suite_2")

    vd_1 = ValidationDefinition(name="my_vd_1", data=bd_1, suite=suite_1)
    vd_2 = ValidationDefinition(name="my_vd_2", data=bd_2, suite=suite_2)

    cp = Checkpoint(name="my_cp", validation_definitions=[vd_1, vd_2])

    diagnostics = cp.is_fresh()
    assert diagnostics.success is False
    assert len(diagnostics.errors) == 5
    assert [type(err) for err in diagnostics.errors] == [
        ExpectationSuiteNotAddedError,
        ValidationDefinitionNotAddedError,
        ExpectationSuiteNotAddedError,
        ValidationDefinitionNotAddedError,
        CheckpointNotAddedError,
    ]


class TestCheckpointPydanticSerializationMethods:
    """
    Test overridden Pydantic serialization methods for Checkpoint
    (dict and json)
    """

    datasource_name: str = "my_pandas_ds"
    asset_name: str = "my_pandas_asset"
    batch_definition_name: str = "my_bd"
    suite_name: str = "my_suite"
    validation_definition_name: str = "my_vd"
    checkpoint_name: str = "my_cp"

    def _build_checkpoint_with_factories(
        self,
        context: AbstractDataContext,
    ) -> Checkpoint:
        datasource = context.data_sources.add_pandas(name=self.datasource_name)
        asset = datasource.add_dataframe_asset(name=self.asset_name)
        bd = asset.add_batch_definition_whole_dataframe(name=self.batch_definition_name)
        suite = context.suites.add(ExpectationSuite(name=self.suite_name))
        vd = context.validation_definitions.add(
            ValidationDefinition(name=self.validation_definition_name, data=bd, suite=suite)
        )
        return context.checkpoints.add(
            Checkpoint(name=self.checkpoint_name, validation_definitions=[vd])
        )

    def _build_checkpoint_without_factories(self) -> Checkpoint:
        bd = BatchDefinition(name=self.batch_definition_name)
        suite = ExpectationSuite(name=self.suite_name)
        vd = ValidationDefinition(name=self.validation_definition_name, data=bd, suite=suite)
        return Checkpoint(name=self.checkpoint_name, validation_definitions=[vd])

    @pytest.mark.unit
    def test_dict_serializes_correctly(self, in_memory_runtime_context):
        context = in_memory_runtime_context
        cp = self._build_checkpoint_with_factories(context)

        dict_val = cp.dict()
        assert dict_val == {
            "actions": [],
            "id": mock.ANY,
            "name": self.checkpoint_name,
            "result_format": "SUMMARY",
            "validation_definitions": [
                {
                    "id": mock.ANY,
                    "name": self.validation_definition_name,
                },
            ],
        }

        for id in (dict_val["id"], dict_val["validation_definitions"][0]["id"]):
            try:
                uuid.UUID(id)
            except TypeError:
                pytest.fail("id is not a valid UUID")

    @pytest.mark.unit
    def test_dict_raises_freshness_errors(self):
        cp = self._build_checkpoint_without_factories()
        with pytest.raises(CheckpointRelatedResourcesFreshnessError) as e:
            cp.dict()

        assert len(e.value.errors) == 3
        assert [type(err) for err in e.value.errors] == [
            BatchDefinitionNotAddedError,
            ExpectationSuiteNotAddedError,
            ValidationDefinitionNotAddedError,
        ]

    @pytest.mark.unit
    def test_json_serializes_correctly(self, in_memory_runtime_context):
        context = in_memory_runtime_context

        cp = self._build_checkpoint_with_factories(context)

        json_val = cp.json()
        dict_val = json.loads(json_val)
        assert dict_val == {
            "actions": [],
            "id": mock.ANY,
            "name": self.checkpoint_name,
            "result_format": "SUMMARY",
            "validation_definitions": [
                {
                    "id": mock.ANY,
                    "name": self.validation_definition_name,
                },
            ],
        }

        for id in (dict_val["id"], dict_val["validation_definitions"][0]["id"]):
            try:
                uuid.UUID(id)
            except TypeError:
                pytest.fail("id is not a valid UUID")

    @pytest.mark.unit
    def test_json_raises_freshness_errors(self):
        cp = self._build_checkpoint_without_factories()
        with pytest.raises(CheckpointRelatedResourcesFreshnessError) as e:
            cp.json()

        assert len(e.value.errors) == 3
        assert [type(err) for err in e.value.errors] == [
            BatchDefinitionNotAddedError,
            ExpectationSuiteNotAddedError,
            ValidationDefinitionNotAddedError,
        ]


@mock.patch(
    "great_expectations.data_context.data_context.context_factory.project_manager.is_using_cloud",
)
@pytest.mark.unit
def test_loaded_checkpoint_can_run(
    mock_is_using_cloud,
    empty_data_context: AbstractDataContext,
):
    mock_is_using_cloud.return_value = True
    col = "col"
    name = "checkpoint_testing"
    bd = (
        empty_data_context.data_sources.add_pandas(name)
        .add_dataframe_asset(name)
        .add_batch_definition_whole_dataframe(name)
    )
    suite = empty_data_context.suites.add(
        ExpectationSuite(
            name="test_suite",
            expectations=[
                gxe.ExpectColumnValuesToBeInSet(
                    column=col,
                    value_set=[1],
                )
            ],
        )
    )
    vd = empty_data_context.validation_definitions.add(
        ValidationDefinition(
            name=name,
            suite=suite,
            data=bd,
        )
    )
    empty_data_context.checkpoints.add(
        Checkpoint(
            name=name,
            validation_definitions=[vd],
        )
    )

    cp = empty_data_context.checkpoints.get(name)

    results = cp.run({"dataframe": pd.DataFrame({col: [1, 1]})})
    assert results.success


@pytest.mark.unit
def test_checkpoint_expectation_parameters(
    empty_data_context: AbstractDataContext,
) -> None:
    col = "col"
    name = "checkpoint_testing"
    bd = (
        empty_data_context.data_sources.add_pandas(name)
        .add_dataframe_asset(name)
        .add_batch_definition_whole_dataframe(name)
    )
    suite = empty_data_context.suites.add(
        ExpectationSuite(
            name="test_suite",
            expectations=[
                gxe.ExpectColumnValuesToBeInSet(
                    column=col,
                    value_set={"$PARAMETER": "values"},
                )
            ],
        )
    )
    vd = empty_data_context.validation_definitions.add(
        ValidationDefinition(
            name=name,
            suite=suite,
            data=bd,
        )
    )
    checkpoint = empty_data_context.checkpoints.add(
        Checkpoint(
            name=name,
            validation_definitions=[vd],
        )
    )

    results = checkpoint.run(
        expectation_parameters={"values": [1, 2]},
        batch_parameters={"dataframe": pd.DataFrame({col: [1, 2]})},
    )
    assert results.success
