from typing import Union

from great_expectations.compatibility.pydantic import (
    BaseModel,
)


class _IdentifierBundle(BaseModel):
    name: str
    id: Union[str, None]
