from __future__ import annotations

import json
import uuid
from unittest import mock

import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.analytics.events import ValidationConfigUpdatedEvent
from great_expectations.core.batch_config import BatchConfig
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.core.validation_config import ValidationConfig
from great_expectations.data_context.data_context.context_factory import ProjectManager
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.datasource.fluent.pandas_datasource import (
    CSVAsset,
    PandasDatasource,
    _PandasDataAsset,
)
from great_expectations.execution_engine.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.validator.v1_validator import (
    OldValidator,
    ResultFormat,
)


@pytest.fixture
def ephemeral_context():
    return gx.get_context(mode="ephemeral")


@pytest.fixture
def validation_config(ephemeral_context: EphemeralDataContext) -> ValidationConfig:
    context = ephemeral_context
    batch_config = (
        context.sources.add_pandas("my_datasource")
        .add_csv_asset("csv_asset", "taxi.csv")  # type: ignore
        .add_batch_config("my_batch_config")
    )
    return ValidationConfig(
        name="my_validation",
        data=batch_config,
        suite=ExpectationSuite(name="my_suite"),
    )


@pytest.fixture
def saved_validation_config(
    ephemeral_context: EphemeralDataContext, validation_config: ValidationConfig
) -> ValidationConfig:
    context = ephemeral_context
    return context.validations.add(validation_config)


class TestValidationRun:
    @pytest.fixture
    def mock_validator(self):
        """Set up our ProjectManager to return a mock Validator"""
        with mock.patch.object(ProjectManager, "get_validator") as mock_get_validator:
            with mock.patch.object(OldValidator, "graph_validate"):
                gx.get_context()
                mock_validator = OldValidator(
                    execution_engine=mock.MagicMock(spec=ExecutionEngine)
                )
                mock_get_validator.return_value = mock_validator

                yield mock_validator

    @pytest.mark.unit
    def test_passes_simple_data_to_validator(
        self,
        mock_validator: mock.MagicMock,
        validation_config: ValidationConfig,
    ):
        validation_config.suite.add_expectation(
            gxe.ExpectColumnMaxToBeBetween(column="foo", max_value=1)
        )
        mock_validator.graph_validate.return_value = [
            ExpectationValidationResult(success=True)
        ]

        validation_config.run()

        mock_validator.graph_validate.assert_called_with(
            configurations=[
                ExpectationConfiguration(
                    expectation_type="expect_column_max_to_be_between",
                    kwargs={"column": "foo", "max_value": 1.0},
                )
            ],
            runtime_configuration={"result_format": "SUMMARY"},
        )

    @mock.patch.object(_PandasDataAsset, "build_batch_request", autospec=True)
    @pytest.mark.unit
    def test_passes_complex_data_to_validator(
        self,
        mock_build_batch_request,
        mock_validator: mock.MagicMock,
        validation_config: ValidationConfig,
    ):
        validation_config.suite.add_expectation(
            gxe.ExpectColumnMaxToBeBetween(
                column="foo", max_value={"$PARAMETER": "max_value"}
            )
        )
        mock_validator.graph_validate.return_value = [
            ExpectationValidationResult(success=True)
        ]

        validation_config.run(
            batch_config_options={"year": 2024},
            evaluation_parameters={"max_value": 9000},
            result_format=ResultFormat.COMPLETE,
        )

        mock_validator.graph_validate.assert_called_with(
            configurations=[
                ExpectationConfiguration(
                    expectation_type="expect_column_max_to_be_between",
                    kwargs={"column": "foo", "max_value": 9000},
                )
            ],
            runtime_configuration={"result_format": "COMPLETE"},
        )

    @pytest.mark.unit
    def test_returns_expected_data(
        self,
        mock_validator: mock.MagicMock,
        validation_config: ValidationConfig,
    ):
        graph_validate_results = [ExpectationValidationResult(success=True)]
        mock_validator.graph_validate.return_value = graph_validate_results

        output = validation_config.run()

        assert output == ExpectationSuiteValidationResult(
            results=graph_validate_results,
            success=True,
            statistics={
                "evaluated_expectations": 1,
                "successful_expectations": 1,
                "unsuccessful_expectations": 0,
                "success_percent": 100.0,
            },
        )


class TestValidationConfigSerialization:
    ds_name = "my_ds"
    asset_name = "my_asset"
    batch_config_name = "my_batch_config"
    suite_name = "my_suite"
    validation_config_name = "my_validation"

    @pytest.fixture
    def validation_config_data(
        self,
        in_memory_runtime_context: EphemeralDataContext,
    ) -> tuple[PandasDatasource, CSVAsset, BatchConfig]:
        context = in_memory_runtime_context

        ds = context.sources.add_pandas(self.ds_name)
        asset = ds.add_csv_asset(self.asset_name, "data.csv")
        batch_config = asset.add_batch_config(self.batch_config_name)

        return ds, asset, batch_config

    @pytest.fixture
    def validation_config_suite(self) -> ExpectationSuite:
        return ExpectationSuite(self.suite_name)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "ds_id, asset_id, batch_config_id",
        [
            (
                "9a88975e-6426-481e-8248-7ce90fad51c4",
                "9b35aa4d-7f01-420d-9d45-b45658e60afd",
                "782c4aaf-8d56-4d8f-9982-49821f4c86c2",
            ),
            (
                None,
                None,
                "782c4aaf-8d56-4d8f-9982-49821f4c86c2",
            ),
            (
                "9a88975e-6426-481e-8248-7ce90fad51c4",
                "9b35aa4d-7f01-420d-9d45-b45658e60afd",
                None,
            ),
            (None, None, None),
        ],
        ids=["with_data_ids", "no_parent_ids", "no_child_id", "without_data_ids"],
    )
    @pytest.mark.parametrize(
        "suite_id",
        ["9b35aa4d-7f01-420d-9d45-b45658e60afd", None],
        ids=["with_suite_id", "without_suite_id"],
    )
    @pytest.mark.parametrize(
        "validation_id",
        ["708bd8b9-1ae4-43e6-8dfc-42ec320aa3db", None],
        ids=["with_validation_id", "without_validation_id"],
    )
    def test_validation_config_serialization(
        self,
        ds_id: str | None,
        asset_id: str | None,
        batch_config_id: str | None,
        suite_id: str | None,
        validation_id: str | None,
        validation_config_data: tuple[PandasDatasource, CSVAsset, BatchConfig],
        validation_config_suite: ExpectationSuite,
    ):
        pandas_ds, csv_asset, batch_config = validation_config_data

        pandas_ds.id = ds_id
        csv_asset.id = asset_id
        batch_config.id = batch_config_id
        validation_config_suite.id = suite_id

        validation_config = ValidationConfig(
            name=self.validation_config_name,
            data=batch_config,
            suite=validation_config_suite,
            id=validation_id,
        )

        actual = json.loads(validation_config.json(models_as_dict=False))
        expected = {
            "name": self.validation_config_name,
            "data": {
                "datasource": {
                    "name": pandas_ds.name,
                    "id": ds_id,
                },
                "asset": {
                    "name": csv_asset.name,
                    "id": asset_id,
                },
                "batch_config": {
                    "name": batch_config.name,
                    "id": batch_config_id,
                },
            },
            "suite": {
                "name": validation_config_suite.name,
                "id": suite_id,
            },
            "id": validation_id,  # TODO: Test to ensure ValidationConfigStore adds a top-level id as well
        }

        # If the suite id is missing, the ExpectationsStore is reponsible for generating and persisting a new one
        if suite_id is None:
            self._assert_contains_valid_uuid(actual["suite"])

        assert actual == expected

    def _assert_contains_valid_uuid(self, data: dict):
        id = data.pop("id")
        data["id"] = mock.ANY
        try:
            uuid.UUID(id)
        except ValueError:
            pytest.fail(f"Expected {id} to be a valid UUID")

    @pytest.mark.unit
    def test_validation_config_deserialization_success(
        self,
        in_memory_runtime_context: EphemeralDataContext,
        validation_config_data: tuple[PandasDatasource, CSVAsset, BatchConfig],
        validation_config_suite: ExpectationSuite,
    ):
        context = in_memory_runtime_context
        _, _, batch_config = validation_config_data

        validation_config_suite = context.suites.add(validation_config_suite)

        serialized_config = {
            "name": self.validation_config_name,
            "data": {
                "datasource": {
                    "name": self.ds_name,
                    "id": None,
                },
                "asset": {
                    "name": self.asset_name,
                    "id": None,
                },
                "batch_config": {
                    "name": self.batch_config_name,
                    "id": None,
                },
            },
            "suite": {
                "name": validation_config_suite.name,
                "id": validation_config_suite.id,
            },
            "id": None,
        }

        validation_config = ValidationConfig.parse_obj(serialized_config)
        assert validation_config.name == self.validation_config_name
        assert validation_config.data == batch_config
        assert validation_config.suite == validation_config_suite

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "serialized_config, error_substring",
        [
            pytest.param(
                {
                    "name": validation_config_name,
                    "data": {
                        "asset": {
                            "name": asset_name,
                            "id": None,
                        },
                        "batch_config": {
                            "name": batch_config_name,
                            "id": None,
                        },
                    },
                    "suite": {
                        "name": suite_name,
                        "id": None,
                    },
                    "id": None,
                },
                "data did not contain expected identifiers",
                id="bad_data_format[missing_datasource]",
            ),
            pytest.param(
                {
                    "name": validation_config_name,
                    "data": {},
                    "suite": {
                        "name": suite_name,
                        "id": None,
                    },
                    "id": None,
                },
                "data did not contain expected identifiers",
                id="bad_data_format[empty_field]",
            ),
            pytest.param(
                {
                    "name": validation_config_name,
                    "data": {
                        "datasource": {
                            "name": ds_name,
                            "id": None,
                        },
                        "asset": {
                            "name": asset_name,
                            "id": None,
                        },
                        "batch_config": {
                            "name": batch_config_name,
                            "id": None,
                        },
                    },
                    "suite": {},
                    "id": None,
                },
                "suite did not contain expected identifiers",
                id="bad_suite_format",
            ),
        ],
    )
    def test_validation_config_deserialization_bad_format(
        self, serialized_config: dict, error_substring: str
    ):
        with pytest.raises(ValueError, match=f"{error_substring}*."):
            ValidationConfig.parse_obj(serialized_config)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "serialized_config, error_substring",
        [
            pytest.param(
                {
                    "name": validation_config_name,
                    "data": {
                        "datasource": {
                            "name": ds_name,
                            "id": None,
                        },
                        "asset": {
                            "name": asset_name,
                            "id": None,
                        },
                        "batch_config": {
                            "name": batch_config_name,
                            "id": None,
                        },
                    },
                    "suite": {
                        "name": "i_do_not_exist",
                        "id": None,
                    },
                    "id": None,
                },
                "Could not find suite",
                id="non_existant_suite",
            ),
            pytest.param(
                {
                    "name": validation_config_name,
                    "data": {
                        "datasource": {
                            "name": "i_do_not_exist",
                            "id": None,
                        },
                        "asset": {
                            "name": asset_name,
                            "id": None,
                        },
                        "batch_config": {
                            "name": batch_config_name,
                            "id": None,
                        },
                    },
                    "suite": {
                        "name": suite_name,
                        "id": None,
                    },
                    "id": None,
                },
                "Could not find datasource",
                id="non_existant_datasource",
            ),
            pytest.param(
                {
                    "name": validation_config_name,
                    "data": {
                        "datasource": {
                            "name": ds_name,
                            "id": None,
                        },
                        "asset": {
                            "name": "i_do_not_exist",
                            "id": None,
                        },
                        "batch_config": {
                            "name": batch_config_name,
                            "id": None,
                        },
                    },
                    "suite": {
                        "name": suite_name,
                        "id": None,
                    },
                    "id": None,
                },
                "Could not find asset",
                id="non_existant_asset",
            ),
            pytest.param(
                {
                    "name": validation_config_name,
                    "data": {
                        "datasource": {
                            "name": ds_name,
                            "id": None,
                        },
                        "asset": {
                            "name": asset_name,
                            "id": None,
                        },
                        "batch_config": {
                            "name": "i_do_not_exist",
                            "id": None,
                        },
                    },
                    "suite": {
                        "name": suite_name,
                        "id": None,
                    },
                    "id": None,
                },
                "Could not find batch config",
                id="non_existant_batch_config",
            ),
        ],
    )
    def test_validation_config_deserialization_non_existant_resource(
        self,
        validation_config_data: tuple[PandasDatasource, CSVAsset, BatchConfig],
        validation_config_suite: ExpectationSuite,
        serialized_config: dict,
        error_substring: str,
    ):
        with pytest.raises(ValueError, match=f"{error_substring}*."):
            ValidationConfig.parse_obj(serialized_config)


@pytest.mark.unit
def test_validation_config_save_success(
    ephemeral_context: EphemeralDataContext, saved_validation_config: ValidationConfig
):
    context = ephemeral_context
    vc = saved_validation_config

    other_suite = ExpectationSuite(name="my_other_suite")
    vc.suite = other_suite
    vc.save()

    assert vc.suite == other_suite
    assert context.validations.get(vc.name).suite == other_suite


@pytest.mark.unit
def test_validation_config_save_failure(validation_config: ValidationConfig):
    with pytest.raises(
        ValueError, match="ValidationConfig must be added to a store before saving."
    ):
        validation_config.save()


@pytest.mark.unit
def test_validation_config_save_emits_analytics_event(
    saved_validation_config: ValidationConfig,
):
    vc = saved_validation_config

    with mock.patch(
        "great_expectations.core.validation_config.submit_event",
        autospec=True,
    ) as mock_submit:
        vc.save()

    # Assert
    mock_submit.assert_called_once_with(
        event=ValidationConfigUpdatedEvent(
            validation_id=vc.id,
            expectation_suite_id=vc.suite.id,
            batch_definition_id=vc.data.id,
        )
    )
