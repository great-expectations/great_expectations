from __future__ import annotations

import json
import uuid
from unittest import mock

import pytest

import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_config import ValidationConfig


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
):
    context = gx.get_context(mode="ephemeral")

    ds_name = "my_ds"
    ds = context.sources.add_pandas(ds_name)
    ds.id = ds_id

    asset_name = "my_asset"
    asset = ds.add_csv_asset(asset_name, "data.csv")
    asset.id = asset_id

    batch_config_name = "my_batch_config"
    batch_config = asset.add_batch_config(batch_config_name)
    batch_config.id = batch_config_id

    suite_name = "my_suite"
    suite = ExpectationSuite(name=suite_name, id=suite_id)

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
                "name": ds_name,
                "id": ds_id,
            },
            "asset": {
                "name": asset_name,
                "id": asset_id,
            },
            "batch_config": {
                "name": batch_config_name,
                "id": batch_config_id,
            },
        },
        "suite": {
            "name": suite_name,
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


def test_validation_config_deserializes_from_ids():
    pass
