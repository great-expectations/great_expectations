from dataclasses import dataclass
from typing import Any, Callable, Dict, Generator, Generic, TypeVar, Union

from great_expectations.compatibility.pydantic import (
    BaseModel,
    ValidationError,
    fields,
    root_validator,
)

RequiredType = TypeVar("RequiredType")
T = TypeVar("T")


@dataclass
class Required(Generic[RequiredType]):
    """Use this Generic whenever you have a field that should be marked as required
    in the JSON schema, but also has a default value to be populated on the form control.

    For more on how pydantic 1.0 can handle Generic classes:
    https://docs.pydantic.dev/1.10/usage/types/#__tabbed_32_1
    """

    required_value: RequiredType

    @classmethod
    def __get_validators__(cls) -> Generator[Callable[[T, fields.ModelField], T], None, None]:
        yield cls.validate

    @classmethod
    def validate(cls, v: T, field: fields.ModelField) -> T:
        if v is None or not field.sub_fields:
            raise ValueError("Required fields cannot be None.")  # noqa: TRY003  # this message isn't long
        if len(field.sub_fields) != 1:
            raise TypeError("Only one type can be passed to `Required`. Try `typing.Union` or `|`.")  # noqa: TRY003  # this message isn't long
        else:
            inner_type = field.sub_fields[0]
            _, error = inner_type.validate(v, {}, loc="required_field")
            if error:
                raise ValidationError([error], cls)  # type: ignore[arg-type]  # seems to be a pydantic bug that this isn't recognized as dataclass
        return v

    @classmethod
    def __modify_schema__(
        cls, field_schema: Dict[str, Any], field: Union[fields.ModelField, None]
    ) -> None:
        if field:
            field.required = True

            # we only allow one type to be passed to Required, so allOf is superfluous
            all_of = field_schema.pop("allOf")
            if len(all_of) != 1:
                raise TypeError(  # noqa: TRY003  # this message isn't long
                    "Only one type can be passed to `Required`. Try `typing.Union` or `|`"
                )
            field_schema.update(all_of[0])

            if field.default:
                field_schema["default"] = field.default


class MinMaxAnyOfValidatorMixin(BaseModel):
    @root_validator(pre=True)
    def any_of(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        error_msg = "At least one of min_value or max_value must be specified"
        if v and v.get("min_value") is None and v.get("max_value") is None:
            raise ValueError(error_msg)
        return v
