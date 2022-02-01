import json
from typing import Any

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import SerializableDictDot
from great_expectations.util import deep_filter_properties_iterable


class SerializableBuilder(SerializableDictDot):
    """
    A SerializableBuilder provides methods to serialize any builder object of a rule generically.
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
