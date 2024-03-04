from __future__ import annotations

import json
import uuid
from unittest import mock

import pytest

from great_expectations.core.batch_config import BatchConfig
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_config import ValidationConfig
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.datasource.fluent.pandas_datasource import (
    CSVAsset,
    PandasDatasource,
)


class TestValidationConfigSerialization:
    ds_name = "my_ds"
    asset_name = "my_asset"
    batch_config_name = "my_batch_config"
    suite_name = "my_suite"
    validation_config_name = "my_validation"

    @pytest.fixture
    def ds_asset_batch_config_bundle(
        self,
        in_memory_runtime_context: EphemeralDataContext,
    ) -> tuple[PandasDatasource, CSVAsset, BatchConfig]:
        context = in_memory_runtime_context

        ds = context.sources.add_pandas(self.ds_name)
        asset = ds.add_csv_asset(self.asset_name, "data.csv")
        batch_config = asset.add_batch_config(self.batch_config_name)

        return ds, asset, batch_config

    @pytest.fixture
    def suite(self) -> ExpectationSuite:
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
            (None, None, None),
        ],
        ids=["with_data_ids", "without_data_ids"],
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
        ds_asset_batch_config_bundle: tuple[PandasDatasource, CSVAsset, BatchConfig],
        suite: ExpectationSuite,
    ):
        pandas_ds, csv_asset, batch_config = ds_asset_batch_config_bundle

        pandas_ds.id = ds_id
        csv_asset.id = asset_id
        batch_config.id = batch_config_id
        suite.id = suite_id

        validation_config = ValidationConfig(
            name=self.validation_config_name,
            data=batch_config,
            suite=suite,
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
                "name": suite.name,
                "id": suite_id,
            },
            "id": validation_id,
        }

        # If the suite id is missing, the ExpectationsStore is reponsible for generating and persisting a new one
        if suite_id is None:
            id = actual["suite"].pop("id")
            actual["suite"]["id"] = mock.ANY
            try:
                uuid.UUID(id)
            except ValueError:
                pytest.fail(f"Expected {id} to be a valid UUID")

        assert actual == expected

    @pytest.mark.unit
    def test_validation_config_deserialization_success(
        self,
        in_memory_runtime_context: EphemeralDataContext,
        ds_asset_batch_config_bundle: tuple[PandasDatasource, CSVAsset, BatchConfig],
        suite: ExpectationSuite,
    ):
        context = in_memory_runtime_context
        pandas_ds, csv_asset, batch_config = ds_asset_batch_config_bundle

        suite = context.suites.add(suite)

        serialized_config = {
            "name": self.validation_config_name,
            "data": {
                "datasource": {
                    "name": pandas_ds.name,
                    "id": None,
                },
                "asset": {
                    "name": csv_asset.name,
                    "id": None,
                },
                "batch_config": {
                    "name": batch_config.name,
                    "id": None,
                },
            },
            "suite": {
                "name": suite.name,
                "id": suite.id,
            },
            "id": None,
        }

        validation_config = ValidationConfig.parse_obj(serialized_config)
        assert validation_config.name == self.validation_config_name
        assert validation_config.data == batch_config
        assert validation_config.suite == suite

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "serialized_config, error_substring",
        [
            pytest.param(
                {
                    "name": "my_validation",
                    "data": {
                        "asset": {
                            "name": "my_asset",
                            "id": None,
                        },
                        "batch_config": {
                            "name": "my_batch_config",
                            "id": None,
                        },
                    },
                    "suite": {
                        "name": "my_suite",
                        "id": None,
                    },
                    "id": None,
                },
                "data did not contain expected identifiers",
                id="bad_data_format[missing_datasource]",
            ),
            pytest.param(
                {
                    "name": "my_validation",
                    "data": {},
                    "suite": {
                        "name": "my_suite",
                        "id": None,
                    },
                    "id": None,
                },
                "data did not contain expected identifiers",
                id="bad_data_format[empty_field]",
            ),
            pytest.param(
                {
                    "name": "my_validation",
                    "data": {
                        "datasource": {
                            "name": "my_ds",
                            "id": None,
                        },
                        "asset": {
                            "name": "my_asset",
                            "id": None,
                        },
                        "batch_config": {
                            "name": "my_batch_config",
                            "id": None,
                        },
                    },
                    "suite": {},
                    "id": None,
                },
                "suite did not contain expected identifiers",
                id="bad_suite_format",
            ),
            pytest.param(
                {
                    "name": "my_validation",
                    "data": {
                        "datasource": {
                            "name": "my_datasource",
                            "id": None,
                        },
                        "asset": {
                            "name": "my_asset",
                            "id": None,
                        },
                        "batch_config": {
                            "name": "my_batch_config",
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
                    "name": "my_validation",
                    "data": {
                        "datasource": {
                            "name": "i_do_not_exist",
                            "id": None,
                        },
                        "asset": {
                            "name": "my_asset",
                            "id": None,
                        },
                        "batch_config": {
                            "name": "my_batch_config",
                            "id": None,
                        },
                    },
                    "suite": {
                        "name": "my_suite",
                        "id": None,
                    },
                    "id": None,
                },
                "Could not find datasource",
                id="non_existant_datasource",
            ),
            pytest.param(
                {
                    "name": "my_validation",
                    "data": {
                        "datasource": {
                            "name": "my_ds",
                            "id": None,
                        },
                        "asset": {
                            "name": "i_do_not_exist",
                            "id": None,
                        },
                        "batch_config": {
                            "name": "my_batch_config",
                            "id": None,
                        },
                    },
                    "suite": {
                        "name": "my_suite",
                        "id": None,
                    },
                    "id": None,
                },
                "Could not find asset",
                id="non_existant_asset",
            ),
            pytest.param(
                {
                    "name": "my_validation",
                    "data": {
                        "datasource": {
                            "name": "my_ds",
                            "id": None,
                        },
                        "asset": {
                            "name": "my_asset",
                            "id": None,
                        },
                        "batch_config": {
                            "name": "i_do_not_exist",
                            "id": None,
                        },
                    },
                    "suite": {
                        "name": "my_suite",
                        "id": None,
                    },
                    "id": None,
                },
                "Could not find batch config",
                id="non_existant_batch_config",
            ),
        ],
    )
    def test_validation_config_deserialization_bad_format(
        self, serialized_config: dict, error_substring: str
    ):
        with pytest.raises(ValueError, match=f"{error_substring}*."):
            ValidationConfig.parse_obj(serialized_config)
