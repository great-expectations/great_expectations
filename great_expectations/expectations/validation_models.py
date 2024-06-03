from typing import Any, Dict

from great_expectations.compatibility.pydantic import (
    BaseModel,
    model_validator,
    root_validator,
)

# model_validator does not exist in pydantic v1
if model_validator is not None:

    class MinMaxAnyOfValidatorMixin(BaseModel):
        @model_validator(mode="before")
        @classmethod
        def any_of(cls, v) -> Dict[str, Any]:
            error_msg = "At least one of min_value or max_value must be specified"
            if v and v.min_value is None and v.max_value is None:
                raise ValueError(error_msg)
            return v
else:

    class MinMaxAnyOfValidatorMixin(BaseModel):  # type: ignore[no-redef]
        @root_validator(pre=True)
        def any_of(cls, v: Dict[str, Any]) -> Dict[str, Any]:
            error_msg = "At least one of min_value or max_value must be specified"
            if v and v.get("min_value") is None and v.get("max_value") is None:
                raise ValueError(error_msg)
            return v
