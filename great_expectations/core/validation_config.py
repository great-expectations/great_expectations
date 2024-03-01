from __future__ import annotations

from typing import Union

from great_expectations._docs_decorators import public_api
from great_expectations.compatibility.pydantic import BaseModel, validator
from great_expectations.core.batch_config import BatchConfig
from great_expectations.core.expectation_suite import (
    ExpectationSuite,
    expectationSuiteSchema,
)
from great_expectations.data_context.data_context.context_factory import project_manager


def _encode_suite(suite: ExpectationSuite) -> dict:
    if not suite.id:
        expectation_store = project_manager.get_expectations_store()
        key = expectation_store.get_key(name=suite.name, id=suite.id)
        expectation_store.add(key=key, value=suite)

    return {"name": suite.name, "id": suite.id}


def _encode_data(data: BatchConfig) -> dict:
    parent = data.data_asset
    grandparent = parent.datasource
    return {
        "datasource": {
            "name": grandparent.name,
            "id": grandparent.id,
        },
        "asset": {
            "name": parent.name,
            "id": parent.id,
        },
        "batch_config": {
            "name": data.name,
            "id": data.id,
        },
    }


class ValidationConfig(BaseModel):
    """
    Responsible for running a suite against data and returning a validation result.

    Args:
        name: The name of the validation.
        data: A batch config to validate.
        suite: A grouping of expectations to validate against the data.
        id: A unique identifier for the validation; added when persisted with a store.

    """

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            ExpectationSuite: lambda e: _encode_suite(e),
            BatchConfig: lambda b: _encode_data(b),
        }

    name: str
    data: BatchConfig  # TODO: Should support a union of Asset | BatchConfig
    suite: ExpectationSuite
    id: Union[str, None] = None

    @validator("suite", pre=True)
    def _validate_suite(cls, v: dict | ExpectationSuite):
        if isinstance(v, dict):
            return cls._encode_suite(v)
        elif isinstance(v, ExpectationSuite):
            return v
        raise ValueError(
            "Suite must be a dictionary (if being deserialized) or an ExpectationSuite object."
        )

    @validator("data", pre=True)
    def _validate_data(cls, v: dict | BatchConfig):
        if isinstance(v, dict):
            return cls._encode_data(v)
        elif isinstance(v, BatchConfig):
            return v
        raise ValueError(
            "Data must be a dictionary (if being deserialized) or a BatchConfig object."
        )

    @classmethod
    def _encode_suite(cls, suite_dict: dict) -> ExpectationSuite:
        name = suite_dict["name"]
        id = suite_dict["id"]
        expectation_store = project_manager.get_expectations_store()
        key = expectation_store.get_key(name=name, id=id)
        config = expectation_store.get(key)
        return ExpectationSuite(**expectationSuiteSchema.load(config))

    @classmethod
    def _encode_data(cls, data_dict: dict) -> BatchConfig:
        ds_name = data_dict["datasource"]["name"]
        asset_name = data_dict["asset"]["name"]
        batch_config_name = data_dict["batch_config"]["name"]
        sources = project_manager.get_datasources()

        ds = sources[ds_name]
        asset = ds.get_asset(asset_name)
        batch_config = asset.get_batch_config(batch_config_name)

        return batch_config

    @public_api
    def run(self):
        raise NotImplementedError
