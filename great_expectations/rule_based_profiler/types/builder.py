import json
# TODO: <Alex>ALEX</Alex>
from typing import List, Set, Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import SerializableDictDot
from great_expectations.util import deep_filter_properties_iterable


# TODO: <Alex>ALEX -- abstract general-purpose methods (e.g., "to_dict()"/"properties", "to_json_dict()", "__repr__()", and "__str__()") to the "DictDot" and "SerializableDictDot" classes.</Alex>
class Builder(SerializableDictDot):
    # TODO: <Alex>ALEX</Alex>
    pass
    # TODO: <Alex>ALEX</Alex>
    """
    A Builder provides methods to serialize any builder object of a rule generically.
    """

    # TODO: <Alex>ALEX</Alex>
    # NON_SERIALIZABLE_PROPERTY_NAMES: Set[str] = {
    #     "data_context",
    # }
    # TODO: <Alex>ALEX</Alex>

    # include_field_names: Set[str] = set()
    # exclude_field_names: Set[str] = set()

    # TODO: <Alex>ALEX</Alex>
    # def to_dict(self) -> dict:
    #     return deep_filter_properties_iterable(properties=super().to_dict())
    # TODO: <Alex>ALEX</Alex>
    # TODO: <Alex>ALEX</Alex>
    # def to_dict(self) -> dict:
    #     # TODO: <Alex>ALEX</Alex>
    #     # dict_obj: dict = {}
    #     # TODO: <Alex>ALEX</Alex>
    #
    #     name: str
    #     # TODO: <Alex>ALEX</Alex>
    #     # field_value: Any
    #     # for field_name in self.field_names:
    #     #     if hasattr(self, f"_{field_name}"):
    #     #         field_value = getattr(self, field_name)
    #     #         dict_obj[field_name] = field_value
    #     # TODO: <Alex>ALEX</Alex>
    #
    #     # TODO: <Alex>ALEX</Alex>
    #     dict_obj: dict = {name: self[name] for name in self.field_names()}
    #     # TODO: <Alex>ALEX</Alex>
    #     # return dict_obj
    #     # TODO: <Alex>ALEX</Alex>
    #     # TODO: <Alex>ALEX</Alex>
    #     deep_filter_properties_iterable(properties=dict_obj, inplace=True)
    #     return dict_obj
    #     # TODO: <Alex>ALEX</Alex>
    # TODO: <Alex>ALEX</Alex>

    # TODO: <Alex>ALEX</Alex>
    # def to_dict(self) -> dict:
    #     dict_obj: dict = {}
    #
    #     field_name: str
    #     field_value: Any
    #     # TODO: <Alex>ALEX_TEST</Alex>
    #     for field_name, field_value in deep_filter_properties_iterable(
    #         properties=super().to_raw_dict(),
    #     ).items():
    #         # exclude non-serializable fields that a rule builder may contain
    #         if (
    #             field_name
    #             not in [
    #                 "_data_context",
    #             ]
    #             and field_name[0] == "_"
    #             and hasattr(self, field_name)
    #         ):
    #             field_name = field_name[1:]
    #             dict_obj[field_name] = field_value
    #
    #     return dict_obj

    # TODO: <Alex>ALEX_TEST -- COULD_BE_SUPERCLASS</Alex>
    # TODO: <Alex>ALEX</Alex>
    def to_json_dict(self) -> dict:
        dict_obj: dict = self.to_dict()
        serializeable_dict: dict = convert_to_json_serializable(data=dict_obj)
        return serializeable_dict
    # TODO: <Alex>ALEX</Alex>

    # TODO: <Alex>ALEX</Alex>
    # def field_names(self) -> Set[str]:
    #     # TODO: <Alex>ALEX</Alex>
    #     # include_keys: Set[str] = self.include_field_names
    #     # exclude_keys: Set[str] = self.exclude_field_names
    #     # TODO: <Alex>ALEX</Alex>
    #     # TODO: <Alex>ALEX</Alex>
    #     # exclude_keys: Set[str] = self.exclude_field_names | Builder.NON_SERIALIZABLE_PROPERTY_NAMES
    #     # TODO: <Alex>ALEX</Alex>
    #     # TODO: <Alex>ALEX</Alex>
    #     # return self.property_names(
    #     #     include_keys=include_keys,
    #     #     exclude_keys=exclude_keys,
    #     # )
    #     # TODO: <Alex>ALEX</Alex>
    #     # TODO: <Alex>ALEX</Alex>
    #     return self.property_names(
    #         include_keys=self.include_field_names,
    #         exclude_keys=self.exclude_field_names,
    #     )
    #     # TODO: <Alex>ALEX</Alex>
    # TODO: <Alex>ALEX</Alex>

    # def keys(self) -> List[str]:
    #     print(f'\n[ALEX_TEST] [BUILDER.@FIELD_NAMES()] SELF: TYPE: {str(type(self))}')
    #     a = super().keys()
    #     print(f'\n[ALEX_TEST] [BUILDER.@FIELD_NAMES()] KEYS: {a} ; TYPE: {str(type(a))}')
    #     key_name: str
    #     return list(
    #         filter(
    #             lambda name: len(name) > 0 and name not in Builder.NON_SERIALIZABLE_PROPERTY_NAMES,
    #             [
    #                 key_name[1:] for key_name in super().keys() if key_name[0] == "_"
    #             ],
    #         )
    #     )

    # TODO: <Alex>ALEX_TEST -- COULD_BE_SUPERCLASS</Alex>
    # TODO: <Alex>ALEX</Alex>
    def __repr__(self) -> str:
        json_dict: dict = self.to_json_dict()
        deep_filter_properties_iterable(
            properties=json_dict,
            inplace=True,
        )
        return json.dumps(json_dict, indent=2)

    # TODO: <Alex>ALEX_TEST -- COULD_BE_SUPERCLASS</Alex>
    # TODO: <Alex>ALEX</Alex>
    def __str__(self) -> str:
        return self.__repr__()
    # TODO: <Alex>ALEX</Alex>


# TODO: <Alex>ALEX</Alex>
# def validate_builder_override_config(builder_config: dict):
#     """
#     In order to insure successful instantiation of custom builder classes using "instantiate_class_from_config()",
#     candidate builder override configurations are required to supply both "class_name" and "module_name" attributes.
#
#     :param builder_config: candidate builder override configuration
#     :raises: ProfilerConfigurationError
#     """
#     if not (
#         isinstance(builder_config, dict)
#         and len(
#             (
#                 set(builder_config.keys())
#                 & {
#                     "class_name",
#                     "module_name",
#                 }
#             )
#         )
#         == 2
#     ):
#         raise ge_exceptions.ProfilerConfigurationError(
#             'Both "class_name" and "module_name" must be specified.'
#         )
# TODO: <Alex>ALEX</Alex>
