from typing import Union

from great_expectations.compatibility.pydantic import (
    BaseModel,
)


class _IdentifierBundle(BaseModel):
    name: str
    id: Union[str, None]


class _EncodedValidationData(BaseModel):
    datasource: _IdentifierBundle
    asset: _IdentifierBundle
    batch_definition: _IdentifierBundle
