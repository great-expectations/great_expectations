from typing import Any, Dict

from great_expectations.compatibility.pydantic import (
    BaseModel,
    root_validator,
)


class MinMaxAnyOfValidatorMixin(BaseModel):
    @root_validator(pre=True)
    def any_of(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        error_msg = "At least one of min_value or max_value must be specified"
        if v and v.get("min_value") is None and v.get("max_value") is None:
            raise ValueError(error_msg)
        return v
