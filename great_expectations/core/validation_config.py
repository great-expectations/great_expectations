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
from great_expectations.datasource.new_datasource import (
    BaseDatasource as LegacyDatasource,
)


class _IdentifierBundle(BaseModel):
    name: str
    id: Union[str, None]


class _EncodedValidationData(BaseModel):
    datasource: _IdentifierBundle
    asset: _IdentifierBundle
    batch_config: _IdentifierBundle


def _encode_suite(suite: ExpectationSuite) -> _IdentifierBundle:
    if not suite.id:
        expectation_store = project_manager.get_expectations_store()
        key = expectation_store.get_key(name=suite.name, id=suite.id)
        expectation_store.add(key=key, value=suite)

    return _IdentifierBundle(name=suite.name, id=suite.id)


def _encode_data(data: BatchConfig) -> _EncodedValidationData:
    asset = data.data_asset
    ds = asset.datasource
    return _EncodedValidationData(
        datasource=_IdentifierBundle(
            name=ds.name,
            id=ds.id,
        ),
        asset=_IdentifierBundle(
            name=asset.name,
            id=str(asset.id) if asset.id else None,
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
        arbitrary_types_allowed = (
            True  # Necessary for compatibility with suite's Marshmallow dep
        )
        """
        When serialized, the suite and data fields should be encoded as a set of identifiers.
        These will be used as foreign keys to retrieve the actual objects from the appropriate stores.

        Example:
        {
            "name": "my_validation",
            "data": {
                "datasource": {
                    "name": "my_datasource",
                    "id": "a758816-64c8-46cb-8f7e-03c12cea1d67"
                },
                "asset": {
                    "name": "my_asset",
                    "id": "b5s8816-64c8-46cb-8f7e-03c12cea1d67"
                },
                "batch_config": {
                    "name": "my_batch_config",
                    "id": "3a758816-64c8-46cb-8f7e-03c12cea1d67"
                }
            },
            "suite": {
                "name": "my_suite",
                "id": "8r2g816-64c8-46cb-8f7e-03c12cea1d67"
            },
            "id": "20dna816-64c8-46cb-8f7e-03c12cea1d67"
        }
        """
        json_encoders = {
            ExpectationSuite: lambda e: _encode_suite(e),
            BatchConfig: lambda b: _encode_data(b),
        }

    name: str
    data: BatchConfig  # TODO: Should support a union of Asset | BatchConfig
    suite: ExpectationSuite
    id: Union[str, None] = None  # TODO: Should be updated when persisted with a store.

    @validator("suite", pre=True)
    def _validate_suite(cls, v: dict | ExpectationSuite):
        # Input will be a dict of identifiers if being deserialized or a suite object if being constructed by a user.
        if isinstance(v, dict):
            return cls._decode_suite(v)
        elif isinstance(v, ExpectationSuite):
            return v
        raise ValueError(
            "Suite must be a dictionary (if being deserialized) or an ExpectationSuite object."
        )

    @validator("data", pre=True)
    def _validate_data(cls, v: dict | BatchConfig):
        # Input will be a dict of identifiers if being deserialized or a rich type if being constructed by a user.
        if isinstance(v, dict):
            return cls._decode_data(v)
        elif isinstance(v, BatchConfig):
            return v
        raise ValueError(
            "Data must be a dictionary (if being deserialized) or a BatchConfig object."
        )

    @classmethod
    def _decode_suite(cls, suite_dict: dict) -> ExpectationSuite:
        # Take in raw JSON, ensure it contains appropriate identifiers, and use them to retrieve the actual suite.
        try:
            suite_identifiers = _IdentifierBundle.parse_obj(suite_dict)
        except ValidationError as e:
            raise ValueError(
                "Serialized suite did not contain expected identifiers"
            ) from e

        name = suite_identifiers.name
        id = suite_identifiers.id

        expectation_store = project_manager.get_expectations_store()
        key = expectation_store.get_key(name=name, id=id)

        try:
            config = expectation_store.get(key)
        except gx_exceptions.InvalidKeyError as e:
            raise ValueError(
                f"Could not find suite with name: {name} and id: {id}"
            ) from e

        return ExpectationSuite(**expectationSuiteSchema.load(config))

    @classmethod
    def _decode_data(cls, data_dict: dict) -> BatchConfig:
        # Take in raw JSON, ensure it contains appropriate identifiers, and use them to retrieve the actual data.
        try:
            data_identifiers = _EncodedValidationData.parse_obj(data_dict)
        except ValidationError as e:
            raise ValueError(
                "Serialized data did not contain expected identifiers"
            ) from e

        ds_name = data_identifiers.datasource.name
        asset_name = data_identifiers.asset.name
        batch_config_name = data_identifiers.batch_config.name

        datasource_dict = project_manager.get_datasources()
        try:
            ds = datasource_dict[ds_name]
        except KeyError as e:
            raise ValueError(f"Could not find datasource named '{ds_name}'.") from e

        # Should never be raised but necessary for type checking until we delete non-FDS support.
        if isinstance(ds, LegacyDatasource):
            raise ValueError("Legacy datasources are not supported.")

        try:
            asset = ds.get_asset(asset_name)
        except LookupError as e:
            raise ValueError(
                f"Could not find asset named '{asset_name}' within '{ds_name}' datasource."
            ) from e

        try:
            batch_config = asset.get_batch_config(batch_config_name)
        except KeyError as e:
            raise ValueError(
                f"Could not find batch config named '{batch_config_name}' within '{asset_name}' asset and '{ds_name}' datasource."
            ) from e

        return batch_config

    @public_api
    def run(self):
        raise NotImplementedError
