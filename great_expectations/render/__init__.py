from __future__ import annotations

import json
from copy import deepcopy
from enum import Enum
from typing import List, Optional, Union

from marshmallow import INCLUDE, Schema, fields, post_dump, post_load

from great_expectations.types import DictDot

from .types import RenderedContent
from .view import DefaultJinjaPageView


class RendererPrefix(str, Enum):
    """Available renderer prefixes"""

    LEGACY = "renderer"
    ATOMIC = "atomic"


class AtomicRendererType(str, Enum):
    """Available atomic renderer types"""

    PRESCRIPTIVE = ".".join([RendererPrefix.ATOMIC, "prescriptive"])
    DIAGNOSTIC = ".".join([RendererPrefix.ATOMIC, "diagnostic"])


class AtomicPrescriptiveRendererType(str, Enum):
    """Available atomic prescriptive renderer names"""

    FAILED = ".".join([AtomicRendererType.PRESCRIPTIVE, "failed"])
    SUMMARY = ".".join([AtomicRendererType.PRESCRIPTIVE, "summary"])

    def __str__(self):
        return self.value


class AtomicDiagnosticRendererType(str, Enum):
    """Available atomic diagnostic renderer names"""

    FAILED = ".".join([AtomicRendererType.DIAGNOSTIC, "failed"])
    OBSERVED_VALUE = ".".join([AtomicRendererType.DIAGNOSTIC, "observed_value"])

    def __str__(self):
        return self.value


class LegacyRendererType(str, Enum):
    """Available legacy renderer types"""

    ANSWER = ".".join([RendererPrefix.LEGACY, "answer"])
    DESCRIPTIVE = ".".join([RendererPrefix.LEGACY, "descriptive"])
    DIAGNOSTIC = ".".join([RendererPrefix.LEGACY, "diagnostic"])
    PRESCRIPTIVE = ".".join([RendererPrefix.LEGACY, "prescriptive"])
    QUESTION = ".".join([RendererPrefix.LEGACY, "question"])


class LegacyPrescriptiveRendererType(str, Enum):
    """Available legacy prescriptive renderer names"""

    SUMMARY = ".".join([LegacyRendererType.PRESCRIPTIVE, "summary"])


class LegacyDiagnosticRendererType(str, Enum):
    """Available legacy diagnostic renderer names"""

    META_PROPERTIES = ".".join([LegacyRendererType.DIAGNOSTIC, "meta_properties"])
    OBSERVED_VALUE = ".".join([LegacyRendererType.DIAGNOSTIC, "observed_value"])
    STATUS_ICON = ".".join([LegacyRendererType.DIAGNOSTIC, "status_icon"])
    SUMMARY = ".".join([LegacyRendererType.DIAGNOSTIC, "summary"])
    UNEXPECTED_STATEMENT = ".".join(
        [LegacyRendererType.DIAGNOSTIC, "unexpected_statement"]
    )
    UNEXPECTED_TABLE = ".".join([LegacyRendererType.DIAGNOSTIC, "unexpected_table"])


class LegacyDescriptiveRendererType(str, Enum):
    """Available legacy descriptive renderer names"""

    COLUMN_PROPERTIES_TABLE_DISTINCT_COUNT_ROW = ".".join(
        [
            LegacyRendererType.DESCRIPTIVE,
            "column_properties_table",
            "distinct_count_row",
        ]
    )
    COLUMN_PROPERTIES_TABLE_DISTINCT_PERCENT_ROW = ".".join(
        [
            LegacyRendererType.DESCRIPTIVE,
            "column_properties_table",
            "distinct_percent_row",
        ]
    )
    COLUMN_PROPERTIES_TABLE_MISSING_COUNT_ROW = ".".join(
        [LegacyRendererType.DESCRIPTIVE, "column_properties_table", "missing_count_row"]
    )
    COLUMN_PROPERTIES_TABLE_MISSING_PERCENT_ROW = ".".join(
        [
            LegacyRendererType.DESCRIPTIVE,
            "column_properties_table",
            "missing_percent_row",
        ]
    )
    COLUMN_PROPERTIES_TABLE_REGEX_COUNT_ROW = ".".join(
        [LegacyRendererType.DESCRIPTIVE, "column_properties_table", "regex_count_row"]
    )
    EXAMPLE_VALUES_BLOCK = ".".join(
        [LegacyRendererType.DESCRIPTIVE, "example_values_block"]
    )
    HISTOGRAM = ".".join([LegacyRendererType.DESCRIPTIVE, "histogram"])
    QUANTILE_TABLE = ".".join([LegacyRendererType.DESCRIPTIVE, "quantile_table"])
    STATS_TABLE_MAX_ROW = ".".join(
        [LegacyRendererType.DESCRIPTIVE, "stats_table", "max_row"]
    )
    STATS_TABLE_MEAN_ROW = ".".join(
        [LegacyRendererType.DESCRIPTIVE, "stats_table", "mean_row"]
    )
    STATS_TABLE_MIN_ROW = ".".join(
        [LegacyRendererType.DESCRIPTIVE, "stats_table", "min_row"]
    )
    VALUE_COUNTS_BAR_CHART = ".".join(
        [LegacyRendererType.DESCRIPTIVE, "value_counts_bar_chart"]
    )


class RenderedAtomicValue(DictDot):
    def __init__(
        self,
        schema: Optional[dict] = None,
        header: Optional[RenderedAtomicValue] = None,
        template: Optional[str] = None,
        params: Optional[dict] = None,
        header_row: Optional[List[RenderedAtomicValue]] = None,
        table: Optional[List[List[RenderedAtomicValue]]] = None,
        graph: Optional[dict] = None,
    ) -> None:
        self.schema: Optional[dict] = schema
        self.header: Optional[RenderedAtomicValue] = header

        # StringValueType
        self.template: Optional[str] = template
        self.params: Optional[dict] = params

        # TableType
        self.header_row: Optional[List[RenderedAtomicValue]] = header_row
        self.table: Optional[List[List[RenderedAtomicValue]]] = table

        # GraphType
        self.graph = RenderedAtomicValueGraph(graph=graph)

    def __repr__(self) -> str:
        return json.dumps(self.to_json_dict(), indent=2)

    def __str__(self) -> str:
        return json.dumps(self.to_json_dict(), indent=2)

    def to_json_dict(self) -> dict:
        """Returns RenderedAtomicValue as a json dictionary."""
        d = renderedAtomicValueSchema.dump(self)
        json_dict: dict = {}
        for key in d:
            if key == "graph":
                json_dict[key] = getattr(self, key).to_json_dict()
            else:
                json_dict[key] = getattr(self, key)
        return json_dict


class RenderedAtomicValueGraph(DictDot):
    def __init__(
        self,
        graph: Optional[dict] = None,
    ):
        self.graph = graph

    def __repr__(self) -> str:
        return json.dumps(self.to_json_dict(), indent=2)

    def __str__(self) -> str:
        return json.dumps(self.to_json_dict(), indent=2)

    def to_json_dict(self) -> Optional[dict]:
        """Returns RenderedAtomicValueGraph as a json dictionary."""
        return self.graph


class RenderedAtomicValueSchema(Schema):
    class Meta:
        unknown = INCLUDE

    schema = fields.Dict(required=False, allow_none=True)
    header = fields.Dict(required=False, allow_none=True)

    # for StringValueType
    template = fields.String(required=False, allow_none=True)
    params = fields.Dict(required=False, allow_none=True)

    # for TableType
    header_row = fields.List(fields.Dict, required=False, allow_none=True)
    table = fields.List(fields.List(fields.Dict, required=False, allow_none=True))

    # for GraphType
    graph = fields.Dict(required=False, allow_none=True)

    @post_load
    def create_value_obj(self, data, **kwargs):
        return RenderedAtomicValue(**data)

    REMOVE_KEYS_IF_NONE = [
        "template",
        "table",
        "params",
        "header_row",
        "table",
        "graph",
    ]

    @post_dump
    def clean_null_attrs(self, data: dict, **kwargs: dict) -> dict:
        """Removes the attributes in RenderedAtomicValueSchema.REMOVE_KEYS_IF_NONE during serialization if
        their values are None."""
        data = deepcopy(data)
        for key in RenderedAtomicValueSchema.REMOVE_KEYS_IF_NONE:
            if (
                key == "graph"
                and key in data
                and data.get(key, {}).get("graph") is None
            ):
                data.pop(key)
            elif key in data and data[key] is None:
                data.pop(key)
        return data


class RenderedAtomicContent(RenderedContent):
    def __init__(
        self,
        name: Union[str, AtomicDiagnosticRendererType, AtomicPrescriptiveRendererType],
        value: RenderedAtomicValue,
        value_type: Optional[str] = None,
    ) -> None:
        # str conversion is performed to ensure Enum value is what is serialized
        self.name = str(name)
        self.value = value
        self.value_type = value_type

    def __repr__(self) -> str:
        return json.dumps(self.to_json_dict(), indent=2)

    def __str__(self) -> str:
        return json.dumps(self.to_json_dict(), indent=2)

    def to_json_dict(self) -> dict:
        """Returns RenderedAtomicContent as a json dictionary."""
        d = super().to_json_dict()
        d["name"] = self.name
        d["value"] = self.value.to_json_dict()
        d["value_type"] = self.value_type
        return d


class RenderedAtomicContentSchema(Schema):
    class Meta:
        unknown: INCLUDE  # type: ignore[valid-type]

    name = fields.String(required=False, allow_none=True)
    value = fields.Nested(RenderedAtomicValueSchema(), required=True, allow_none=False)
    value_type = fields.String(required=True, allow_none=False)

    @post_load
    def make_rendered_atomic_content(self, data, **kwargs):
        return RenderedAtomicContent(**data)


renderedAtomicValueSchema = RenderedAtomicValueSchema()
