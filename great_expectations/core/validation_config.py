from __future__ import annotations

from typing import Union

from great_expectations._docs_decorators import public_api
from great_expectations.compatibility.pydantic import BaseModel, validator
from great_expectations.core.batch_config import BatchConfig
from great_expectations.core.expectation_suite import (
    ExpectationSuite,
)
from great_expectations.data_context.data_context.context_factory import project_manager


def _encode_suite(suite: ExpectationSuite) -> str:
    if not suite.id:
        expectation_store = project_manager.get_expectations_store()
        expectation_store.add(suite)

    return suite.id


def _encode_data(data: BatchConfig) -> str:
    if id := data.id:
        return id

    # ds = data.data_asset.datasource
    # sources = project_manager.get_sources()

    raise ValueError("TBD")


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
    def _validate_suite(cls, v):
        if isinstance(v, str):
            return cls._get_suite_by_id(v)
        elif isinstance(v, ExpectationSuite):
            return v
        raise ValueError(
            "Suite must be a dictionary (if being deserialized) or an ExpectationSuite object."
        )

    @classmethod
    def _get_suite_by_id(cls, suite_id: str) -> ExpectationSuite:
        expectation_store = project_manager.get_expectations_store()
        return expectation_store.get(suite_id)

    @public_api
    def run(self):
        raise NotImplementedError
