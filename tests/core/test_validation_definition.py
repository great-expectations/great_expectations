from __future__ import annotations

import datetime
import json
import threading
import uuid
from typing import TYPE_CHECKING, Type
from unittest import mock
from unittest.mock import ANY

import pandas as pd
import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations import RunIdentifier
from great_expectations import __version__ as GX_VERSION
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.core.result_format import ResultFormat
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext
from great_expectations.data_context.data_context.context_factory import (
    set_context,
)
from great_expectations.data_context.store.validation_results_store import ValidationResultsStore
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)
from great_expectations.datasource.fluent.pandas_datasource import (
    CSVAsset,
    PandasDatasource,
    _PandasDataAsset,
)
from great_expectations.exceptions import (
    BatchDefinitionNotAddedError,
    ExpectationSuiteNotAddedError,
    ResourceFreshnessError,
    ValidationDefinitionNotAddedError,
    ValidationDefinitionRelatedResourcesFreshnessError,
)
from great_expectations.exceptions.exceptions import (
    BatchDefinitionNotFoundError,
    ExpectationSuiteNotFoundError,
    ValidationDefinitionNotFoundError,
)
from great_expectations.exceptions.resource_freshness import ResourceFreshnessAggregateError
from great_expectations.execution_engine.execution_engine import ExecutionEngine
from great_expectations.validator.v1_validator import (
    OldValidator,
)
from great_expectations.validator.v1_validator import Validator as V1Validator

if TYPE_CHECKING:
    from unittest.mock import MagicMock  # noqa: TID251 # FIXME CoP

    from pytest_mock import MockerFixture

    from great_expectations.data_context.data_context.ephemeral_data_context import (
        EphemeralDataContext,
    )

BATCH_ID = "my_batch_id"
DATA_SOURCE_NAME = "my_datasource"
ASSET_NAME = "csv_asset"
BATCH_DEFINITION_NAME = "my_batch_definition"
ACTIVE_BATCH_SPEC = {
    "type": "table",
    "data_asset_name": ASSET_NAME,
    "table_name": "test_table",
    "schema_name": "test_schema",
    "batch_identifiers": {"date": {"year": 2017, "month": 12, "day": 3}},
    "partitioner_method": "partition_on_year_and_month_and_day",
    "partitioner_kwargs": {"column_name": "date"},
}
ACTIVE_BATCH_DEFINITION = {
    "datasource_name": DATA_SOURCE_NAME,
    "data_connector_name": "fluent",
    "data_asset_name": ASSET_NAME,
    "batch_identifiers": {"date": {"year": 2017, "month": 12, "day": 3}},
}
BATCH_MARKERS = {"ge_load_time": "20240814T172846.050804Z"}


@pytest.fixture
def ephemeral_context():
    return gx.get_context(mode="ephemeral")


@pytest.fixture
def validation_definition(ephemeral_context: EphemeralDataContext) -> ValidationDefinition:
    context = ephemeral_context
    batch_definition = (
        context.data_sources.add_pandas(DATA_SOURCE_NAME)
        .add_csv_asset(ASSET_NAME, "taxi.csv")  # type: ignore[arg-type] # FIXME CoP
        .add_batch_definition(BATCH_DEFINITION_NAME)
    )
    return context.validation_definitions.add(
        ValidationDefinition(
            name="my_validation",
            data=batch_definition,
            suite=context.suites.add(ExpectationSuite(name="my_suite")),
        )
    )


@pytest.fixture
def dataframe_validation_definition(
    ephemeral_context: EphemeralDataContext,
) -> ValidationDefinition:
    context = ephemeral_context
    batch_definition = (
        context.data_sources.add_pandas(DATA_SOURCE_NAME)
        .add_dataframe_asset("dataframe_asset")
        .add_batch_definition_whole_dataframe("dataframe_batch_def")
    )
    return context.validation_definitions.add(
        ValidationDefinition(
            name="my_dataframe_validation",
            data=batch_definition,
            suite=context.suites.add(ExpectationSuite(name="my_suite")),
        )
    )


@pytest.fixture
def two_dataframe_validation_definitions(
    ephemeral_context: EphemeralDataContext,
) -> dict[str, tuple[ValidationDefinition, pd.DataFrame]]:
    """Two assets on one pandas datasource, each with a row-count suite matching its frame."""
    context = ephemeral_context
    datasource = context.data_sources.add_pandas(DATA_SOURCE_NAME)
    out = {}
    for name, rows in (("small", 3), ("big", 10)):
        batch_definition = datasource.add_dataframe_asset(
            name
        ).add_batch_definition_whole_dataframe("whole")
        suite = context.suites.add(ExpectationSuite(name=f"suite-{name}"))
        suite.add_expectation(gxe.ExpectTableRowCountToEqual(value=rows))
        validation_definition = context.validation_definitions.add(
            ValidationDefinition(name=f"vd-{name}", data=batch_definition, suite=suite)
        )
        out[name] = (validation_definition, pd.DataFrame({"x": range(rows)}))
    return out


@pytest.fixture
def postgres_validation_definition(
    ephemeral_context: EphemeralDataContext,
) -> ValidationDefinition:
    context = ephemeral_context
    batch_definition = (
        ephemeral_context.data_sources.add_postgres(
            name="postgres_datasource",
            connection_string="postgresql+psycopg2://postgres:postgres@localhost:5432/test_ci",
        )
        .add_query_asset(name="my_asset", query="SELECT * FROM fake_table")
        .add_batch_definition_monthly(
            name="my_batch_definition",
            column="not_very_real",
            # We skip validating we can generate batches because fake_table is fake
            # and won't validate.
            validate_batchable=False,
        )
    )
    return context.validation_definitions.add(
        ValidationDefinition(
            name="my_postgres_validation",
            data=batch_definition,
            suite=context.suites.add(ExpectationSuite(name="my_suite")),
        )
    )


@pytest.mark.unit
def test_validation_definition_data_properties(validation_definition: ValidationDefinition):
    assert validation_definition.data.name == BATCH_DEFINITION_NAME
    assert validation_definition.batch_definition.name == BATCH_DEFINITION_NAME
    assert validation_definition.asset.name == ASSET_NAME
    assert validation_definition.data_source.name == DATA_SOURCE_NAME


class TestValidationRun:
    @pytest.fixture
    def mock_validator(self, mocker: MockerFixture):
        """Set up the Data Context to return a mock Validator"""
        with mock.patch.object(AbstractDataContext, "get_validator") as mock_get_validator:
            with mock.patch.object(OldValidator, "graph_validate"):
                gx.get_context(mode="ephemeral")
                mock_execution_engine = mocker.MagicMock(spec=ExecutionEngine)
                # The Validator reports the identity of the Batch it loaded, not whatever
                # the shared execution engine last saw, so the identity lives on the Batch.
                mock_batch = mocker.MagicMock(
                    id=BATCH_ID,
                    batch_spec=ACTIVE_BATCH_SPEC,
                    batch_definition=ACTIVE_BATCH_DEFINITION,
                    batch_markers=BATCH_MARKERS,
                )
                mock_validator = OldValidator(
                    execution_engine=mock_execution_engine, batches=[mock_batch]
                )
                mock_get_validator.return_value = mock_validator

                yield mock_validator

    @pytest.mark.unit
    def test_passes_simple_data_to_validator(
        self,
        mock_validator: MagicMock,
        validation_definition: ValidationDefinition,
    ):
        validation_definition.suite.add_expectation(
            gxe.ExpectColumnMaxToBeBetween(column="foo", max_value=1)
        )
        mock_validator.graph_validate.return_value = [ExpectationValidationResult(success=True)]

        validation_definition.run()

        mock_validator.graph_validate.assert_called_once()
        assert len(mock_validator.graph_validate.call_args[1]["configurations"]) == 1
        assert (
            mock_validator.graph_validate.call_args[1]["configurations"][0]["type"]
            == "expect_column_max_to_be_between"
        )
        assert mock_validator.graph_validate.call_args[1]["configurations"][0]["kwargs"] == {
            "column": "foo",
            "max_value": 1.0,
        }
        assert mock_validator.graph_validate.call_args[1]["runtime_configuration"] == {
            "result_format": "SUMMARY"
        }

    @mock.patch.object(_PandasDataAsset, "build_batch_request", autospec=True)
    @pytest.mark.unit
    def test_passes_complex_data_to_validator(
        self,
        mock_build_batch_request,
        mock_validator: MagicMock,
        validation_definition: ValidationDefinition,
    ):
        validation_definition.suite.add_expectation(
            gxe.ExpectColumnMaxToBeBetween(column="foo", max_value={"$PARAMETER": "max_value"})
        )
        mock_validator.graph_validate.return_value = [ExpectationValidationResult(success=True)]

        validation_definition.run(
            batch_parameters={"year": 2024},
            expectation_parameters={"max_value": 9000},
            result_format=ResultFormat.COMPLETE,
        )

        mock_validator.graph_validate.assert_called_once()
        assert len(mock_validator.graph_validate.call_args[1]["configurations"]) == 1
        assert (
            mock_validator.graph_validate.call_args[1]["configurations"][0]["type"]
            == "expect_column_max_to_be_between"
        )
        assert mock_validator.graph_validate.call_args[1]["configurations"][0]["kwargs"] == {
            "column": "foo",
            "max_value": 9000,
        }
        assert mock_validator.graph_validate.call_args[1]["runtime_configuration"] == {
            "result_format": "COMPLETE"
        }

    @pytest.mark.unit
    def test_returns_expected_data(
        self,
        mock_validator: MagicMock,
        validation_definition: ValidationDefinition,
    ):
        graph_validate_results = [ExpectationValidationResult(success=True)]
        mock_validator.graph_validate.return_value = graph_validate_results

        output = validation_definition.run()

        # Ignore meta for purposes of this test
        output["meta"] = {}
        assert output == ExpectationSuiteValidationResult(
            results=graph_validate_results,
            success=True,
            suite_name="empty_suite",
            statistics={
                "evaluated_expectations": 1,
                "successful_expectations": 1,
                "unsuccessful_expectations": 0,
                "success_percent": 100.0,
            },
            meta={},
        )

    @pytest.mark.parametrize("checkpoint_id", [None, "my_checkpoint_id"])
    @pytest.mark.unit
    def test_adds_requisite_fields(
        self,
        mock_validator: MagicMock,
        validation_definition: ValidationDefinition,
        checkpoint_id: str | None,
    ):
        mock_validator.graph_validate.return_value = []

        output = validation_definition.run(checkpoint_id=checkpoint_id)

        assert output.meta == {
            "validation_id": validation_definition.id,
            "checkpoint_id": checkpoint_id,
            "batch_parameters": None,
            "batch_spec": ACTIVE_BATCH_SPEC,
            "batch_markers": BATCH_MARKERS,
            "active_batch_definition": ACTIVE_BATCH_DEFINITION,
            "great_expectations_version": GX_VERSION,
            "run_id": ANY,
            "validation_time": ANY,
        }
        assert isinstance(output.meta["run_id"], RunIdentifier)
        assert isinstance(output.meta["validation_time"], datetime.datetime)

    @pytest.mark.unit
    def test_adds_correct_batch_parameter_field_for_dataframes(
        self,
        mock_validator: MagicMock,
        dataframe_validation_definition: ValidationDefinition,
    ) -> None:
        mock_validator.graph_validate.return_value = []

        output = dataframe_validation_definition.run(
            checkpoint_id=None,
            batch_parameters={"dataframe": pd.DataFrame({"a": ["1", "2", "3", "4", "5"]})},
        )

        assert output.meta == {
            "validation_id": dataframe_validation_definition.id,
            "checkpoint_id": None,
            "batch_parameters": {"dataframe": "<DATAFRAME>"},
            "batch_spec": ACTIVE_BATCH_SPEC,
            "batch_markers": BATCH_MARKERS,
            "active_batch_definition": ACTIVE_BATCH_DEFINITION,
            "great_expectations_version": GX_VERSION,
            "run_id": ANY,
            "validation_time": ANY,
        }
        assert isinstance(output.meta["run_id"], RunIdentifier)
        assert isinstance(output.meta["validation_time"], datetime.datetime)

    @pytest.mark.parametrize(
        "batch_parameters",
        [
            pytest.param(None),
            pytest.param({"year": 2024}),
            pytest.param({"year": 2024, "month": 10}),
        ],
    )
    @pytest.mark.postgresql
    def test_adds_correct_batch_parameter_fields_for_postgres(
        self,
        mock_validator: MagicMock,
        postgres_validation_definition: ValidationDefinition,
        batch_parameters: dict | None,
    ) -> None:
        mock_validator.graph_validate.return_value = []

        output = postgres_validation_definition.run(
            checkpoint_id=None,
            batch_parameters=batch_parameters,
        )

        assert output.meta == {
            "validation_id": postgres_validation_definition.id,
            "checkpoint_id": None,
            "batch_parameters": batch_parameters,
            "batch_spec": ACTIVE_BATCH_SPEC,
            "batch_markers": BATCH_MARKERS,
            "active_batch_definition": ACTIVE_BATCH_DEFINITION,
            "great_expectations_version": GX_VERSION,
            "run_id": ANY,
            "validation_time": ANY,
        }
        assert isinstance(output.meta["run_id"], RunIdentifier)
        assert isinstance(output.meta["validation_time"], datetime.datetime)

    @mock.patch.object(ValidationResultsStore, "set")
    @pytest.mark.unit
    def test_persists_validation_results_for_non_cloud(
        self,
        mock_validation_results_store_set: MagicMock,
        mock_validator: MagicMock,
        validation_definition: ValidationDefinition,
    ):
        validation_definition.suite.add_expectation(
            gxe.ExpectColumnMaxToBeBetween(column="foo", max_value=1)
        )
        mock_validator.graph_validate.return_value = [ExpectationValidationResult(success=True)]

        validation_definition.run()

        mock_validator.graph_validate.assert_called_once()
        assert len(mock_validator.graph_validate.call_args[1]["configurations"]) == 1
        assert (
            mock_validator.graph_validate.call_args[1]["configurations"][0]["type"]
            == "expect_column_max_to_be_between"
        )
        assert mock_validator.graph_validate.call_args[1]["configurations"][0]["kwargs"] == {
            "column": "foo",
            "max_value": 1.0,
        }
        assert mock_validator.graph_validate.call_args[1]["runtime_configuration"] == {
            "result_format": "SUMMARY"
        }

        # validate we are calling set on the store with data that's roughly the right shape
        [(_, kwargs)] = mock_validation_results_store_set.call_args_list
        key = kwargs["key"]
        value = kwargs["value"]
        assert isinstance(key, ValidationResultIdentifier)
        assert key.batch_identifier == BATCH_ID
        assert value.success is True

    @pytest.mark.unit
    def test_dependencies_not_added_raises_error(self, validation_definition: ValidationDefinition):
        validation_definition.suite.id = None
        validation_definition.data.id = None

        with pytest.raises(ValidationDefinitionRelatedResourcesFreshnessError) as e:
            validation_definition.run()

        assert [type(err) for err in e.value.errors] == [
            BatchDefinitionNotAddedError,
            ExpectationSuiteNotAddedError,
        ]


class TestValidationDefinitionSerialization:
    ds_name = "my_ds"
    asset_name = "my_asset"
    batch_definition_name = "my_batch_definition"
    suite_name = "my_suite"
    validation_definition_name = "my_validation"

    @pytest.fixture
    def context(self, in_memory_runtime_context: EphemeralDataContext) -> EphemeralDataContext:
        return in_memory_runtime_context

    @pytest.fixture
    def validation_definition_data(
        self,
        context: EphemeralDataContext,
    ) -> tuple[PandasDatasource, CSVAsset, BatchDefinition]:
        ds = context.data_sources.add_pandas(self.ds_name)
        # `filepath_or_buffer` is typed as a FilePath, which requires the file to
        # exist. This asset is never read in these tests, so a placeholder path is
        # deliberate and the resulting arg-type mismatch is expected.
        asset = ds.add_csv_asset(self.asset_name, "data.csv")  # type: ignore[arg-type]
        batch_definition = asset.add_batch_definition(self.batch_definition_name)

        return ds, asset, batch_definition

    @pytest.fixture
    def validation_definition_suite(self, context: EphemeralDataContext) -> ExpectationSuite:
        return context.suites.add(ExpectationSuite(self.suite_name))

    @pytest.mark.unit
    def test_validation_definition_serialization(
        self,
        in_memory_runtime_context: EphemeralDataContext,
        validation_definition_data: tuple[PandasDatasource, CSVAsset, BatchDefinition],
        validation_definition_suite: ExpectationSuite,
    ):
        context = in_memory_runtime_context
        pandas_ds, csv_asset, batch_definition = validation_definition_data

        ds_id = uuid.uuid4()
        pandas_ds.id = ds_id

        asset_id = uuid.uuid4()
        csv_asset.id = asset_id

        batch_definition_id = str(uuid.uuid4())
        batch_definition.id = batch_definition_id

        suite_id = str(uuid.uuid4())
        validation_definition_suite.id = suite_id

        validation_definition = context.validation_definitions.add(
            ValidationDefinition(
                name=self.validation_definition_name,
                data=batch_definition,
                suite=validation_definition_suite,
            )
        )

        actual = json.loads(validation_definition.json(models_as_dict=False))
        expected = {
            "name": self.validation_definition_name,
            "data": {
                "datasource": {
                    "name": pandas_ds.name,
                    "id": str(ds_id),
                },
                "asset": {
                    "name": csv_asset.name,
                    "id": str(asset_id),
                },
                "batch_definition": {
                    "name": batch_definition.name,
                    "id": batch_definition_id,
                },
            },
            "suite": {
                "name": validation_definition_suite.name,
                "id": suite_id,
            },
            "id": mock.ANY,
        }

        assert actual == expected
        assert actual["id"] is not None

    def _assert_contains_valid_uuid(self, data: dict):
        id = data.pop("id")
        data["id"] = mock.ANY
        try:
            uuid.UUID(id)
        except ValueError:
            pytest.fail(f"Expected {id} to be a valid UUID")

    @pytest.mark.unit
    def test_validation_definition_deserialization_success(
        self,
        context: EphemeralDataContext,
        validation_definition_data: tuple[PandasDatasource, CSVAsset, BatchDefinition],
        validation_definition_suite: ExpectationSuite,
    ):
        _, _, batch_definition = validation_definition_data

        serialized_config = {
            "name": self.validation_definition_name,
            "data": {
                "datasource": {
                    "name": self.ds_name,
                    "id": None,
                },
                "asset": {
                    "name": self.asset_name,
                    "id": None,
                },
                "batch_definition": {
                    "name": self.batch_definition_name,
                    "id": None,
                },
            },
            "suite": {
                "name": validation_definition_suite.name,
                "id": validation_definition_suite.id,
            },
            "id": None,
        }

        validation_definition = ValidationDefinition.parse_obj(serialized_config)
        assert validation_definition.name == self.validation_definition_name
        assert validation_definition.data == batch_definition
        assert validation_definition.suite == validation_definition_suite

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "serialized_config, error_substring",
        [
            pytest.param(
                {
                    "name": validation_definition_name,
                    "data": {
                        "asset": {
                            "name": asset_name,
                            "id": None,
                        },
                        "batch_definition": {
                            "name": batch_definition_name,
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
                    "name": validation_definition_name,
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
                    "name": validation_definition_name,
                    "data": {
                        "datasource": {
                            "name": ds_name,
                            "id": None,
                        },
                        "asset": {
                            "name": asset_name,
                            "id": None,
                        },
                        "batch_definition": {
                            "name": batch_definition_name,
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
    def test_validation_definition_deserialization_bad_format(
        self, context: EphemeralDataContext, serialized_config: dict, error_substring: str
    ):
        with pytest.raises(ValueError, match=f"{error_substring}*."):
            ValidationDefinition.parse_obj(serialized_config)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "serialized_config, error_substring",
        [
            pytest.param(
                {
                    "name": validation_definition_name,
                    "data": {
                        "datasource": {
                            "name": ds_name,
                            "id": None,
                        },
                        "asset": {
                            "name": asset_name,
                            "id": None,
                        },
                        "batch_definition": {
                            "name": batch_definition_name,
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
                    "name": validation_definition_name,
                    "data": {
                        "datasource": {
                            "name": "i_do_not_exist",
                            "id": None,
                        },
                        "asset": {
                            "name": asset_name,
                            "id": None,
                        },
                        "batch_definition": {
                            "name": batch_definition_name,
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
                    "name": validation_definition_name,
                    "data": {
                        "datasource": {
                            "name": ds_name,
                            "id": None,
                        },
                        "asset": {
                            "name": "i_do_not_exist",
                            "id": None,
                        },
                        "batch_definition": {
                            "name": batch_definition_name,
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
                    "name": validation_definition_name,
                    "data": {
                        "datasource": {
                            "name": ds_name,
                            "id": None,
                        },
                        "asset": {
                            "name": asset_name,
                            "id": None,
                        },
                        "batch_definition": {
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
                "Could not find batch definition",
                id="non_existant_batch_definition",
            ),
        ],
    )
    def test_validation_definition_deserialization_non_existant_resource(
        self,
        validation_definition_data: tuple[PandasDatasource, CSVAsset, BatchDefinition],
        validation_definition_suite: ExpectationSuite,
        serialized_config: dict,
        error_substring: str,
    ):
        with pytest.raises(ValueError, match=f"{error_substring}*."):
            ValidationDefinition.parse_obj(serialized_config)


@pytest.mark.unit
def test_identifier_bundle_no_id_raises_error(validation_definition: ValidationDefinition):
    validation_definition.id = None

    with pytest.raises(ValidationDefinitionRelatedResourcesFreshnessError):
        validation_definition.identifier_bundle()


@pytest.mark.unit
def test_save_success(
    mocker: MockerFixture,
    validation_definition: ValidationDefinition,
    ephemeral_context: EphemeralDataContext,
):
    context = mocker.Mock(spec=AbstractDataContext)
    set_context(project=context)

    # The validation definition belongs to the ephemeral context, so it saves there.
    owner_store = ephemeral_context.validation_definition_store
    update = mocker.patch.object(owner_store, "update")
    store_key = owner_store.get_key(name=validation_definition.name, id=validation_definition.id)
    validation_definition.save()

    update.assert_called_once_with(key=store_key, value=validation_definition)
    context.validation_definition_store.update.assert_not_called()


@pytest.mark.parametrize(
    "has_id,has_suite_id,has_batch_def_id,error_list",
    [
        pytest.param(
            True,
            True,
            True,
            [],
            id="validation_id|suite_id|batch_def_id",
        ),
        pytest.param(
            True,
            False,
            True,
            [ExpectationSuiteNotAddedError],
            id="validation_id|no_suite_id|batch_def_id",
        ),
        pytest.param(
            True,
            True,
            False,
            [BatchDefinitionNotAddedError],
            id="validation_id|suite_id|no_batch_def_id",
        ),
        pytest.param(
            True,
            False,
            False,
            [BatchDefinitionNotAddedError, ExpectationSuiteNotAddedError],
            id="validation_id|no_suite_id|no_batch_def_id",
        ),
        pytest.param(
            False,
            True,
            True,
            [ValidationDefinitionNotAddedError],
            id="no_validation_id|suite_id|batch_def_id",
        ),
        pytest.param(
            False,
            False,
            True,
            [ExpectationSuiteNotAddedError, ValidationDefinitionNotAddedError],
            id="no_validation_id|no_suite_id|batch_def_id",
        ),
        pytest.param(
            False,
            True,
            False,
            [BatchDefinitionNotAddedError, ValidationDefinitionNotAddedError],
            id="no_validation_id|suite_id|no_batch_def_id",
        ),
        pytest.param(
            False,
            False,
            False,
            [
                BatchDefinitionNotAddedError,
                ExpectationSuiteNotAddedError,
                ValidationDefinitionNotAddedError,
            ],
            id="no_validation_id|no_suite_id|no_batch_def_id",
        ),
    ],
)
@pytest.mark.unit
def test_is_fresh(
    in_memory_runtime_context,
    has_id: bool,
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

    # Stores/Fluent API will always assign IDs but we manually override them here
    # for purposes of changing object state for the test
    if not has_batch_def_id:
        validation_definition.data.id = None
    if not has_suite_id:
        validation_definition.suite.id = None
    if not has_id:
        validation_definition.id = None

    diagnostics = validation_definition.is_fresh()
    try:
        diagnostics.raise_for_error()
    except ResourceFreshnessAggregateError as e:
        assert [type(err) for err in e.errors] == error_list


@pytest.mark.unit
def test_is_fresh_raises_error_when_validation_definition_not_found(in_memory_runtime_context):
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

    context.validation_definitions.delete(validation_definition.name)

    diagnostics = validation_definition.is_fresh()
    assert diagnostics.success is False
    assert len(diagnostics.errors) == 1
    assert isinstance(diagnostics.errors[0], ValidationDefinitionNotFoundError)


class TestGetUnexpectedRows:
    @pytest.mark.unit
    def test_raises_value_error_for_non_unexpected_rows_expectation(
        self, validation_definition: ValidationDefinition
    ):
        expectation = gxe.ExpectColumnValuesToNotBeNull(column="foo")
        with pytest.raises(ValueError, match="Only UnexpectedRowsExpectation"):
            validation_definition.get_unexpected_rows(expectation)

    @pytest.mark.unit
    def test_calls_compute_metrics_with_correct_args(
        self, validation_definition: ValidationDefinition, mocker: MockerFixture
    ):
        expectation = gxe.UnexpectedRowsExpectation(
            unexpected_rows_query="SELECT * FROM {batch} WHERE col > 5"
        )
        mock_batch = mocker.MagicMock()
        mock_metric_result = mocker.MagicMock()
        mock_metric_result.value = [{"col": 10}]
        mock_batch.compute_metrics.return_value = mock_metric_result

        mocker.patch.object(
            BatchDefinition,
            "get_batch",
            return_value=mock_batch,
        )

        result = validation_definition.get_unexpected_rows(expectation)
        assert result == [{"col": 10}]
        mock_batch.compute_metrics.assert_called_once()
        call_arg = mock_batch.compute_metrics.call_args[0][0]
        assert call_arg.name == "query.table"
        assert call_arg.query == "SELECT * FROM {batch} WHERE col > 5"
        assert call_arg.fetch_all is True

    @pytest.mark.unit
    def test_passes_batch_parameters(
        self, validation_definition: ValidationDefinition, mocker: MockerFixture
    ):
        expectation = gxe.UnexpectedRowsExpectation(
            unexpected_rows_query="SELECT * FROM {batch} WHERE col > 5"
        )
        mock_batch = mocker.MagicMock()
        mock_metric_result = mocker.MagicMock()
        mock_metric_result.value = []
        mock_batch.compute_metrics.return_value = mock_metric_result

        mock_get_batch = mocker.patch.object(
            BatchDefinition,
            "get_batch",
            return_value=mock_batch,
        )

        batch_params = {"year": 2026, "month": 3}
        validation_definition.get_unexpected_rows(expectation, batch_parameters=batch_params)
        mock_get_batch.assert_called_once_with(batch_params)

    @pytest.mark.unit
    def test_resolves_suite_parameter_query(
        self, validation_definition: ValidationDefinition, mocker: MockerFixture
    ):
        expectation = gxe.UnexpectedRowsExpectation(
            unexpected_rows_query={"$PARAMETER": "my_query"}
        )
        mock_batch = mocker.MagicMock()
        mock_metric_result = mocker.MagicMock()
        mock_metric_result.value = [{"col": 99}]
        mock_batch.compute_metrics.return_value = mock_metric_result

        mocker.patch.object(
            BatchDefinition,
            "get_batch",
            return_value=mock_batch,
        )

        result = validation_definition.get_unexpected_rows(
            expectation,
            expectation_parameters={"my_query": "SELECT * FROM {batch} WHERE col > 5"},
        )
        assert result == [{"col": 99}]
        call_arg = mock_batch.compute_metrics.call_args[0][0]
        assert call_arg.query == "SELECT * FROM {batch} WHERE col > 5"
        assert call_arg.fetch_all is True

    @pytest.mark.unit
    def test_raises_when_suite_parameter_missing(self, validation_definition: ValidationDefinition):
        expectation = gxe.UnexpectedRowsExpectation(
            unexpected_rows_query={"$PARAMETER": "my_query"}
        )
        with pytest.raises(ValueError, match="no expectation_parameters were provided"):
            validation_definition.get_unexpected_rows(expectation)


@pytest.mark.unit
def test_is_fresh_raises_error_when_child_deps_not_found(in_memory_runtime_context):
    context = in_memory_runtime_context

    datasource = context.data_sources.add_pandas(name="my_pandas_ds")
    asset = datasource.add_csv_asset(name="my_csv_asset", filepath_or_buffer="data.csv")
    batch_definition = asset.add_batch_definition(name="my_batch_def")
    suite = context.suites.add(ExpectationSuite(name="my_suite"))
    validation_definition = context.validation_definitions.add(
        ValidationDefinition(
            name="my_validation_definition",
            suite=suite,
            data=batch_definition,
        )
    )

    asset.delete_batch_definition(batch_definition.name)
    context.suites.delete(suite.name)

    diagnostics = validation_definition.is_fresh()
    assert diagnostics.success is False
    assert len(diagnostics.errors) == 2
    assert isinstance(diagnostics.errors[0], BatchDefinitionNotFoundError)
    assert isinstance(diagnostics.errors[1], ExpectationSuiteNotFoundError)


# Shorter than the per-test timeout the suite runs under, so a thread that never gets its
# turn unwinds and reports inside its own test instead of being left behind for the next one.
_CONCURRENT_RUN_WAIT_SECONDS = 1.0


class TestConcurrentValidationRuns:
    """Two ValidationDefinitions on one datasource share its cached execution engine.

    The interleaving these tests force is the one a thread pool produces on its own: both
    Validators exist before either builds its metric graph (or assembles its result). A
    barrier makes it deterministic; it does not create the shared state.

    Where the sync points sit is worth stating because it once mattered on Python 3.10 and
    3.11. The wrapped Validator is built under ``V1Validator._wrapped_validator``, which caches
    per instance; it used to be a ``functools.cached_property``, whose ``__get__`` held one
    class-wide lock while computing on those versions (3.12 removed the lock,
    python/cpython#87634). A thread that waited for the other thread *inside* that computation
    held the lock the other thread needed to build its own Validator, and both hung. The
    placements below do not depend on that history: a wait for the other thread's Validator
    goes at ``graph_validate``, the first call after the property has been computed, or earlier
    still at ``_validate_expectation_configs``, before the property is touched at all. Neither
    placement ever sits inside ``__init__``, so no wait runs while another build is in flight.
    """

    @staticmethod
    def _run_on_threads(
        validation_definitions: dict[str, tuple[ValidationDefinition, pd.DataFrame]],
    ) -> dict[str, ExpectationSuiteValidationResult]:
        results: dict[str, ExpectationSuiteValidationResult] = {}
        errors: dict[str, BaseException] = {}

        def run(name: str) -> None:
            validation_definition, dataframe = validation_definitions[name]
            try:
                results[name] = validation_definition.run(batch_parameters={"dataframe": dataframe})
            except BaseException as e:  # re-raised on the main thread below
                errors[name] = e

        threads = [
            threading.Thread(target=run, args=(name,), name=name, daemon=True)
            for name in validation_definitions
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=_CONCURRENT_RUN_WAIT_SECONDS + 0.5)
        stuck = [thread.name for thread in threads if thread.is_alive()]
        assert not stuck, f"threads still running after the wait budget: {stuck}"
        assert not errors, errors
        assert set(results) == set(validation_definitions)
        return results

    @staticmethod
    def _assert_each_result_is_its_own(
        results: dict[str, ExpectationSuiteValidationResult],
    ) -> None:
        for name, rows in (("small", 3), ("big", 10)):
            result = results[name]
            assert result.success is True, (name, result.results[0].result)
            assert result.results[0].result["observed_value"] == rows
            assert result.batch_id == f"{DATA_SOURCE_NAME}-{name}"
            assert result.meta["active_batch_definition"]["data_asset_name"] == name

    @pytest.mark.unit
    def test_runs_that_build_their_validators_together_each_validate_their_own_batch(
        self,
        two_dataframe_validation_definitions: dict[str, tuple[ValidationDefinition, pd.DataFrame]],
        monkeypatch: pytest.MonkeyPatch,
    ):
        both_built = threading.Barrier(2)
        original_graph_validate = OldValidator.graph_validate

        def wait_for_the_other_validator_then_build_the_graph(self, *args, **kwargs):
            # This thread's Validator exists by the time it gets here; the barrier holds its
            # graph build until the other thread's Validator exists too.
            both_built.wait(timeout=_CONCURRENT_RUN_WAIT_SECONDS)
            return original_graph_validate(self, *args, **kwargs)

        monkeypatch.setattr(
            OldValidator, "graph_validate", wait_for_the_other_validator_then_build_the_graph
        )

        results = self._run_on_threads(two_dataframe_validation_definitions)

        self._assert_each_result_is_its_own(results)

    @pytest.mark.unit
    def test_run_whose_metrics_finish_while_another_validator_is_built_reports_its_own_batch(
        self,
        two_dataframe_validation_definitions: dict[str, tuple[ValidationDefinition, pd.DataFrame]],
        monkeypatch: pytest.MonkeyPatch,
    ):
        """The other Validator is built after this run's metrics resolve and before it
        assembles its result, which is where the result's batch identity is read."""
        small_resolved = threading.Event()
        big_built = threading.Event()
        original_init = OldValidator.__init__
        original_graph_validate = OldValidator.graph_validate
        original_validate_expectation_configs = V1Validator._validate_expectation_configs

        def big_waits_for_small_before_building_its_validator(self, *args, **kwargs):
            # Waits before "big" ever touches `_wrapped_validator`, keeping this wait out of
            # another Validator's build. It also pins the ordering the test asserts: "small"'s
            # Validator already exists by the time this returns.
            if threading.current_thread().name == "big":
                assert small_resolved.wait(timeout=_CONCURRENT_RUN_WAIT_SECONDS)
            return original_validate_expectation_configs(self, *args, **kwargs)

        def set_big_built_after_init(self, *args, **kwargs) -> None:
            original_init(self, *args, **kwargs)
            if threading.current_thread().name == "big":
                big_built.set()

        def small_waits_for_big_before_returning(self, *args, **kwargs):
            result = original_graph_validate(self, *args, **kwargs)
            if threading.current_thread().name == "small":
                small_resolved.set()
                assert big_built.wait(timeout=_CONCURRENT_RUN_WAIT_SECONDS)
            return result

        monkeypatch.setattr(
            V1Validator,
            "_validate_expectation_configs",
            big_waits_for_small_before_building_its_validator,
        )
        monkeypatch.setattr(OldValidator, "__init__", set_big_built_after_init)
        monkeypatch.setattr(OldValidator, "graph_validate", small_waits_for_big_before_returning)

        results = self._run_on_threads(two_dataframe_validation_definitions)

        self._assert_each_result_is_its_own(results)
