from __future__ import annotations

import json
import pathlib
import uuid
from typing import TYPE_CHECKING, List
from unittest import mock

import pytest

import great_expectations as gx
from great_expectations import expectations as gxe
from great_expectations import set_context
from great_expectations.checkpoint.actions import (
    MicrosoftTeamsNotificationAction,
    OpsgenieAlertAction,
    PagerdutyAlertAction,
    SlackNotificationAction,
    UpdateDataDocsAction,
    ValidationAction,
)
from great_expectations.checkpoint.v1_checkpoint import (
    Checkpoint,
    CheckpointAction,
    CheckpointResult,
)
from great_expectations.compatibility.pydantic import ValidationError
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.core.result_format import ResultFormat
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
from tests.test_utils import working_directory

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


@pytest.mark.unit
def test_checkpoint_no_validation_definitions_raises_error():
    with pytest.raises(ValueError) as e:
        Checkpoint(name="my_checkpoint", validation_definitions=[])

    assert "Checkpoint must contain at least one validation definition" in str(e.value)


@pytest.mark.unit
def test_checkpoint_save_success(mocker: MockerFixture):
    context = mocker.Mock(spec=AbstractDataContext)
    set_context(project=context)

    checkpoint = Checkpoint(
        name="my_checkpoint",
        validation_definitions=[mocker.Mock(spec=ValidationDefinition)],
    )
    store_key = context.v1_checkpoint_store.get_key.return_value
    checkpoint.save()

    context.v1_checkpoint_store.update.assert_called_once_with(key=store_key, value=checkpoint)


@pytest.fixture
def slack_action():
    return SlackNotificationAction(
        slack_webhook="slack_webhook",
    )


@pytest.fixture
def teams_action():
    return MicrosoftTeamsNotificationAction(
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
        with mock.patch.object(
            ValidationDefinition,
            "json",
            return_value=json.dumps({"id": str(uuid.uuid4()), "name": name}),
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
        with mock.patch.object(
            ValidationDefinition,
            "json",
            return_value=json.dumps({"id": str(uuid.uuid4()), "name": name}),
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

        ds = context.sources.add_pandas(ds_name)
        asset = ds.add_csv_asset(asset_name, "my_file.csv")  # type: ignore[arg-type]

        bc1 = asset.add_batch_definition(batch_definition_name_1)
        suite1 = ExpectationSuite(suite_name_1)
        vc1 = ValidationDefinition(name=validation_definition_name_1, data=bc1, suite=suite1)

        bc2 = asset.add_batch_definition(batch_definition_name_2)
        suite2 = ExpectationSuite(suite_name_2)
        vc2 = ValidationDefinition(name=validation_definition_name_2, data=bc2, suite=suite2)

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
                    "validation_definitions": [],
                    "actions": [],
                    "id": "c758816-64c8-46cb-8f7e-03c12cea1d67",
                },
                "Checkpoint must contain at least one validation definition",
                id="missing_validations",
            ),
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
                {"site_names": [], "type": "update_data_docs"},
                {"slack_webhook": "test", "type": "slack"},
                {"teams_webhook": "test", "type": "microsoft"},
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
        return suite

    @pytest.fixture
    def mock_batch_def(self, mocker: MockerFixture):
        return mocker.Mock(spec=BatchDefinition)

    @pytest.fixture
    def validation_definition(self, mock_suite: MockerFixture, mock_batch_def: MockerFixture):
        validation_definition = ValidationDefinition(
            name=self.validation_definition_name,
            data=mock_batch_def,
            suite=mock_suite,
        )

        mock_run_result = ExpectationSuiteValidationResult(
            success=True,
            results=[
                ExpectationValidationResult(
                    success=True,
                    expectation_config=ExpectationConfiguration(
                        expectation_type="expect_column_values_to_be_between",
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
    def test_checkpoint_run_no_actions(self, validation_definition: ValidationDefinition):
        checkpoint = Checkpoint(
            name=self.checkpoint_name, validation_definitions=[validation_definition]
        )
        result = checkpoint.run()

        run_results = result.run_results
        assert len(run_results) == len(checkpoint.validation_definitions)

        validation_result = tuple(run_results.values())[0]
        assert validation_result.success is True
        assert len(validation_result.results) == 1 and validation_result.results[0].success is True

        assert result.checkpoint_config == checkpoint
        assert result.success is True

    @pytest.mark.unit
    def test_checkpoint_run_actions(
        self,
        validation_definition: ValidationDefinition,
        mocker: MockerFixture,
    ):
        # Arrange
        action = mocker.Mock(spec=UpdateDataDocsAction)
        checkpoint = Checkpoint(
            name=self.checkpoint_name,
            validation_definitions=[validation_definition],
            actions=[action],
        )

        # Act
        result = checkpoint.run()

        # Assert
        action._copy_and_set_values().v1_run.assert_called_once_with(
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
        pd_action = PagerdutyAlertAction(api_key="api_key", routing_key="routing_key")
        og_action = OpsgenieAlertAction(api_key="api_key")
        data_docs_action = UpdateDataDocsAction()
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
        checkpoint = Checkpoint(
            name=self.checkpoint_name, validation_definitions=[validation_definition]
        )
        batch_parameters = {"my_param": "my_value"}
        expectation_parameters = {"my_other_param": "my_other_value"}
        _ = checkpoint.run(
            batch_parameters=batch_parameters, expectation_parameters=expectation_parameters
        )

        validation_definition.run.assert_called_with(  # type: ignore[attr-defined]
            batch_parameters=batch_parameters,
            suite_parameters=expectation_parameters,
            result_format=ResultFormat.SUMMARY,
            run_id=mock.ANY,
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

    @pytest.mark.filesystem
    def test_checkpoint_run_filesytem_e2e(self, tmp_path: pathlib.Path):
        with working_directory(tmp_path):
            context = gx.get_context(mode="file")

        ds = context.sources.add_pandas(self.datasource_name)
        csv_path = (
            pathlib.Path(__file__).parent.parent
            / "test_sets"
            / "quickstart"
            / "yellow_tripdata_sample_2022-01.csv"
        )
        assert csv_path.exists()
        asset = ds.add_csv_asset(self.asset_name, filepath_or_buffer=csv_path)

        batch_definition = asset.add_batch_definition(self.batch_definition_name)
        suite = ExpectationSuite(
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

        validation_definition = ValidationDefinition(
            name=self.validation_definition_name, data=batch_definition, suite=suite
        )

        checkpoint = Checkpoint(
            name=self.checkpoint_name, validation_definitions=[validation_definition]
        )
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
