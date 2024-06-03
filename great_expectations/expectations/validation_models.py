from typing import Any, Dict

from great_expectations.compatibility.pydantic import (
    BaseModel,
    root_validator,
)

# from great_expectations.compatibility.pydantic_core import CoreSchema, core_schema

# RequiredType = TypeVar("RequiredType")
# T = TypeVar("T")
#
#
# @dataclass
# class Required(Generic[RequiredType]):
#     """Use this Generic whenever you have a field that should be marked as required
#     in the JSON schema, but also has a default value to be populated on the form control.
#
#     For more on how pydantic can handle Generic classes:
#     https://docs.pydantic.dev/latest/concepts/types/#handling-custom-generic-classes
#     """
#
#     required_value: RequiredType
#
#     @classmethod
#     def __get_pydantic_core_schema__(
#         cls, source_type: Any, handler: "GetCoreSchemaHandler"
#     ) -> CoreSchema:
#         origin = get_origin(source_type)
#         if origin is None:  # used as `x: Owner` without params
#             origin = source_type
#             item_tp = Any
#         else:
#             item_tp = get_args(source_type)[0]
#         # both calling handler(...) and handler.generate_schema(...)
#         # would work, but prefer the latter for conceptual and consistency reasons
#         item_schema = handler.generate_schema(item_tp)
#
#         def val_item(v: Required[Any], handler: "ValidatorFunctionWrapHandler") -> Required[Any]:
#             v.item = handler(v.item)
#             return v
#
#         python_schema = core_schema.chain_schema(
#             # `chain_schema` means do the following steps in order:
#             [
#                 # Ensure the value is an instance of Required
#                 core_schema.is_instance_schema(cls),
#                 # Use the item_schema to validate `items`
#                 core_schema.no_info_wrap_validator_function(val_item, item_schema),
#             ]
#         )
#
#         return core_schema.json_or_python_schema(
#             # for JSON accept an object with name and item keys
#             json_schema=core_schema.chain_schema(
#                 [
#                     core_schema.typed_dict_schema(
#                         {
#                             "required_value": core_schema.typed_dict_field(item_schema),
#                         }
#                     ),
#                     # after validating the json data convert it to python
#                     core_schema.no_info_before_validator_function(
#                         lambda data: Required(required_value=data["required_value"]),
#                         # note that we re-use the same schema here as below
#                         python_schema,
#                     ),
#                 ]
#             ),
#             python_schema=python_schema,
#         )

#     @classmethod
#     def __get_pydantic_json_schema__(
#         cls, core_schema: CoreSchema, handler: "GetJsonSchemaHandler"
#     ) -> "JsonSchemaValue":
#         json_schema = handler(core_schema)
#         json_schema = handler.resolve_ref_schema(json_schema)
#         json_schema["required"] = True
#         return json_schema


class MinMaxAnyOfValidatorMixin(BaseModel):
    @root_validator(pre=True)
    def any_of(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        error_msg = "At least one of min_value or max_value must be specified"
        if v and v.get("min_value") is None and v.get("max_value") is None:
            raise ValueError(error_msg)
        return v
