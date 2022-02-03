import json
from typing import Any

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import SerializableDictDot
from great_expectations.util import deep_filter_properties_iterable


# TODO: <Alex>ALEX -- abstract general-purpose methods (e.g., "to_dict()"/"properties", "to_json_dict()", "__repr__()", and "__str__()") to the "DictDot" and "SerializableDictDot" classes.</Alex>
class Builder(SerializableDictDot):
    """
    A Builder provides methods to serialize any builder object of a rule generically.
    """

    def to_dict(self) -> dict:
        dict_obj: dict = {}

        field_name: str
        field_value: Any
        for field_name, field_value in deep_filter_properties_iterable(
            properties=super().to_dict(),
        ).items():
            # exclude non-serializable fields that a rule builder may contain
            if (
                field_name
                not in [
                    "_data_context",
                ]
                and field_name[0] == "_"
                and hasattr(self, field_name)
            ):
                field_name = field_name[1:]
                dict_obj[field_name] = field_value

        return dict_obj

    def to_json_dict(self) -> dict:
        dict_obj: dict = self.to_dict()
        serializeable_dict: dict = convert_to_json_serializable(data=dict_obj)
        return serializeable_dict

    def __repr__(self) -> str:
        json_dict: dict = self.to_json_dict()
        return json.dumps(json_dict, indent=2)

    def __str__(self) -> str:
        return self.__repr__()


def validate_builder_override_config(builder_config: dict):
    """
    In order to insure successful instantiation of custom builder classes using "instantiate_class_from_config()",
    candidate builder override configurations are required to supply both "class_name" and "module_name" attributes.

    :param builder_config: candidate builder override configuration
    :raises: ProfilerConfigurationError
    """
    if not (
        isinstance(builder_config, dict)
        and len(
            (
                set(builder_config.keys())
                & {
                    "class_name",
                    "module_name",
                }
            )
        )
        == 2
    ):
        raise ge_exceptions.ProfilerConfigurationError(
            'Both "class_name" and "module_name" must be specified.'
        )
