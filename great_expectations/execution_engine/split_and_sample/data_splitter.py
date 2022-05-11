from __future__ import annotations

import abc
import datetime
import enum
from typing import Callable, List, Union

import ruamel
from dateutil.parser import parse
from ruamel.yaml import yaml_object

import great_expectations.exceptions as ge_exceptions

yaml = ruamel.yaml.YAML()


@yaml_object(yaml)
class DatePart(enum.Enum):
    "SQL supported date parts for most dialects."
    YEAR = "year"
    MONTH = "month"
    WEEK = "week"
    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"

    def __eq__(self, other: DatePart):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.value == other.value

    def __hash__(self: DatePart):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return hash(self.value)

    @classmethod
    def to_yaml(cls, representer, node):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Method allows for yaml-encodable representation of ENUM, using internal methods of ruamel.\n        pattern was found in the following stackoverflow thread:\n        https://stackoverflow.com/questions/48017317/can-ruamel-yaml-encode-an-enum\n        "
        return representer.represent_str(data=node.value)


class DataSplitter(abc.ABC):
    "Abstract base class containing methods for splitting data accessible via Execution Engines.\n\n    Note, for convenience, you can also access DatePart via the instance variable\n    date_part e.g. DataSplitter.date_part.MONTH\n"
    date_part: DatePart = DatePart

    def get_splitter_method(self, splitter_method_name: str) -> Callable:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Get the appropriate splitter method from the method name.\n\n        Args:\n            splitter_method_name: name of the splitter to retrieve.\n\n        Returns:\n            splitter method.\n        "
        splitter_method_name: str = self._get_splitter_method_name(splitter_method_name)
        return getattr(self, splitter_method_name)

    def _get_splitter_method_name(self, splitter_method_name: str) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Accept splitter methods with or without starting with `_`.\n\n        Args:\n            splitter_method_name: splitter name starting with or without preceding `_`.\n\n        Returns:\n            splitter method name stripped of preceding underscore.\n        "
        if splitter_method_name.startswith("_"):
            return splitter_method_name[1:]
        else:
            return splitter_method_name

    def _convert_date_parts(
        self, date_parts: Union[(List[DatePart], List[str])]
    ) -> List[DatePart]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Convert a list of date parts to DatePart objects.\n\n        Args:\n            date_parts: List of DatePart or string representations of DatePart.\n\n        Returns:\n            List of DatePart objects\n        "
        return [
            (DatePart(date_part.lower()) if isinstance(date_part, str) else date_part)
            for date_part in date_parts
        ]

    @staticmethod
    def _validate_date_parts(date_parts: Union[(List[DatePart], List[str])]) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Validate that date parts exist and are of the correct type.\n\n        Args:\n            date_parts: DatePart instances or str.\n\n        Returns:\n            None, this method raises exceptions if the config is invalid.\n        "
        if len(date_parts) == 0:
            raise ge_exceptions.InvalidConfigError(
                "date_parts are required when using split_on_date_parts."
            )
        if not all(
            (isinstance(dp, DatePart) or isinstance(dp, str)) for dp in date_parts
        ):
            raise ge_exceptions.InvalidConfigError(
                "date_parts should be of type DatePart or str."
            )

    @staticmethod
    def _verify_all_strings_are_valid_date_parts(date_part_strings: List[str]) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Verify date part strings by trying to load as DatePart instances.\n\n        Args:\n            date_part_strings: A list of strings that should correspond to DatePart.\n\n        Returns:\n            None, raises an exception if unable to convert.\n        "
        try:
            [DatePart(date_part_string) for date_part_string in date_part_strings]
        except ValueError as e:
            raise ge_exceptions.InvalidConfigError(
                f"{e} please only specify strings that are supported in DatePart: {[dp.value for dp in DatePart]}"
            )

    def _convert_datetime_batch_identifiers_to_date_parts_dict(
        self,
        column_batch_identifiers: Union[(datetime.datetime, str, dict)],
        date_parts: List[DatePart],
    ) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        'Convert batch identifiers to a dict of {date_part as str: date_part value}.\n\n        Args:\n            column_batch_identifiers: Batch identifiers related to the column of interest.\n            date_parts: List of DatePart to include in the return value.\n\n        Returns:\n            A dict of {date_part as str: date_part value} eg. {"day": 3}.\n        '
        if isinstance(column_batch_identifiers, str):
            column_batch_identifiers: datetime.datetime = parse(
                column_batch_identifiers
            )
        if isinstance(column_batch_identifiers, datetime.datetime):
            return {
                date_part.value: getattr(column_batch_identifiers, date_part.value)
                for date_part in date_parts
            }
        else:
            self._verify_all_strings_are_valid_date_parts(
                list(column_batch_identifiers.keys())
            )
            return column_batch_identifiers
