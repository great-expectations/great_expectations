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


@pytest.fixture
def ds_asset_batch_config_bundle(
    in_memory_runtime_context: EphemeralDataContext,
) -> tuple[PandasDatasource, CSVAsset, BatchConfig]:
    context = in_memory_runtime_context

    ds_name = "my_ds"
    ds = context.sources.add_pandas(ds_name)

    asset_name = "my_asset"
    asset = ds.add_csv_asset(asset_name, "data.csv")

    batch_config_name = "my_batch_config"
    batch_config = asset.add_batch_config(batch_config_name)

    return ds, asset, batch_config


@pytest.fixture
def suite():
    suite_name = "my_suite"
    return ExpectationSuite(name=suite_name)


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
)
@pytest.mark.parametrize("suite_id", ["9b35aa4d-7f01-420d-9d45-b45658e60afd", None])
@pytest.mark.parametrize(
    "validation_id", ["708bd8b9-1ae4-43e6-8dfc-42ec320aa3db", None]
)
def test_validation_config_serialization(
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

    validation_name = "my_validation_config"
    validation_config = ValidationConfig(
        name=validation_name,
        data=batch_config,
        suite=suite,
        id=validation_id,
    )

    actual = json.loads(validation_config.json(models_as_dict=False))
    expected = {
        "name": validation_name,
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


def test_validation_config_deserialization(
    in_memory_runtime_context: EphemeralDataContext,
    ds_asset_batch_config_bundle: tuple[PandasDatasource, CSVAsset, BatchConfig],
    suite: ExpectationSuite,
):
    context = in_memory_runtime_context
    pandas_ds, csv_asset, batch_config = ds_asset_batch_config_bundle

    suite = context.suites.add(suite)

    validation_name = "my_validation_config"
    serialized_config = {
        "name": validation_name,
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
    assert validation_config.name == validation_name
    assert validation_config.data == batch_config
    assert validation_config.suite == suite
