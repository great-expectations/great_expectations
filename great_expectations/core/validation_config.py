from __future__ import annotations

from typing import Union

import great_expectations.exceptions as gx_exceptions
from great_expectations._docs_decorators import public_api
from great_expectations.compatibility.pydantic import (
    BaseModel,
    ValidationError,
    validator,
)
from great_expectations.core.batch_config import BatchConfig
from great_expectations.core.expectation_suite import (
    ExpectationSuite,
    expectationSuiteSchema,
)
from great_expectations.data_context.data_context.context_factory import project_manager


class _IdentifierBundle(BaseModel):
    name: str
    id: Union[str, None]


class _EncodedData(BaseModel):
    datasource: _IdentifierBundle
    asset: _IdentifierBundle
    batch_config: _IdentifierBundle


def _encode_suite(suite: ExpectationSuite) -> _IdentifierBundle:
    if not suite.id:
        expectation_store = project_manager.get_expectations_store()
        key = expectation_store.get_key(name=suite.name, id=suite.id)
        expectation_store.add(key=key, value=suite)

    return _IdentifierBundle(name=suite.name, id=suite.id)


def _encode_data(data: BatchConfig) -> _EncodedData:
    asset = data.data_asset
    ds = asset.datasource
    return _EncodedData(
        datasource=_IdentifierBundle(
            name=ds.name,
            id=ds.id,
        ),
        asset=_IdentifierBundle(
            name=asset.name,
            id=asset.id,
        ),
        batch_config=_IdentifierBundle(
            name=data.name,
            id=data.id,
        ),
    )


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
        # When serialized, the suite and data fields should be encoded as a set of identifiers.
        # These will be used as foreign keys to retrieve the actual objects from the appropriate stores.
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
        # Input will be a dict of identifiers if being deserialized or a suite object if being constructed by a user.
        if isinstance(v, dict):
            return cls._encode_suite(v)
        elif isinstance(v, ExpectationSuite):
            return v
        raise ValueError(
            "Suite must be a dictionary (if being deserialized) or an ExpectationSuite object."
        )

    @validator("data", pre=True)
    def _validate_data(cls, v: dict | BatchConfig):
        # Input will be a dict of identifiers if being deserialized or a rich type if being constructed by a user.
        if isinstance(v, dict):
            return cls._encode_data(v)
        elif isinstance(v, BatchConfig):
            return v
        raise ValueError(
            "Data must be a dictionary (if being deserialized) or a BatchConfig object."
        )

    @classmethod
    def _encode_suite(cls, suite_dict: dict) -> ExpectationSuite:
        # Take in raw JSON, ensure it contains appropriate identifiers, and use them to retrieve the actual suite.
        try:
            suite_identifiers = _IdentifierBundle.parse_obj(suite_dict)
        except ValidationError as e:
            raise e  # TODO: Add context and re-raise

        name = suite_identifiers.name
        id = suite_identifiers.id

        expectation_store = project_manager.get_expectations_store()
        key = expectation_store.get_key(name=name, id=id)

        try:
            config = expectation_store.get(key)
        except gx_exceptions.InvalidKeyError as e:
            raise e  # TODO: Add context and re-raise

        return ExpectationSuite(**expectationSuiteSchema.load(config))

    @classmethod
    def _encode_data(cls, data_dict: dict) -> BatchConfig:
        # Take in raw JSON, ensure it contains appropriate identifiers, and use them to retrieve the actual data.
        try:
            data_identifiers = _EncodedData.parse_obj(data_dict)
        except ValidationError as e:
            raise e  # TODO: Add context and re-raise

        ds_name = data_identifiers.datasource.name
        asset_name = data_identifiers.asset.name
        batch_config_name = data_identifiers.batch_config.name

        datasource_dict = project_manager.get_datasources()
        try:
            ds = datasource_dict[ds_name]
        except KeyError as e:
            raise e  # TODO: Add context and re-raise

        try:
            asset = ds.get_asset(asset_name)
        except LookupError as e:
            raise e  # TODO: Add context and re-raise

        try:
            batch_config = asset.get_batch_config(batch_config_name)
        except KeyError as e:
            raise e  # TODO: Add context and re-raise

        return batch_config

    @public_api
    def run(self):
        raise NotImplementedError
