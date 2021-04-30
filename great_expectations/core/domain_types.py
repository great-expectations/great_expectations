from enum import Enum
from typing import Any, Callable, Dict, List, Optional, cast

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.util import MetaClsEnumJoin
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator


class DomainTypes(Enum):
    """
    This is a base class for the different specific DomainTypes classes, each of which enumerates the particular variety
    of domain types (e.g., "StructuredDomainTypes", "SemanticDomainTypes", "MetricDomainTypes", etc.).  Since the base
    "DomainTypes" extends "Enum", the JSON serialization, supported for the general "Enum" class, applies for all
    "DomainTypes" classes, too.
    """

    @classmethod
    def has_member_key(cls, key: Any) -> bool:
        key_exists: bool = key in cls.__members__
        if key_exists:
            hash_op: Optional[Callable] = getattr(key, "__hash__", None)
            if callable(hash_op):
                return True
            return False
        return False


class StructuredDomainTypes(DomainTypes):
    SCHEMA = "schema"
    TABLE = "table"
    COLUMN = "column"
    COLUMN_PAIR = "column_pair"
    MULTICOLUMN = "multicolumn"


class SemanticDomainTypes(DomainTypes):
    IDENTITY = "identity"
    NUMERIC = "numeric"
    DATETIME = "datetime"
    TEXT = "text"
    LOGIC = "logic"
    CURRENCY = "currency"
    IMAGE = "image"
    VALUE_SET = "value_set"
    MISCELLANEOUS = "miscellaneous"
    UNKNOWN = "unknown"


class MetricDomainTypes(
    Enum, metaclass=MetaClsEnumJoin, enums=(StructuredDomainTypes, SemanticDomainTypes)
):
    pass


def translate_column_type_to_semantic_domain_type(
    validator: Validator, column_name: str
) -> SemanticDomainTypes:
    # Note: As of Python 3.8, specifying argument type in Lambda functions is not supported by Lambda syntax.
    column_types_dict_list: List[Dict[str, Any]] = list(
        filter(
            lambda column_type_dict: column_name in column_type_dict,
            validator.get_metric(
                metric=MetricConfiguration(
                    metric_name="table.column_types",
                    metric_domain_kwargs={},
                    metric_value_kwargs={
                        "include_nested": True,
                    },
                    metric_dependencies=None,
                )
            ),
        )
    )
    if len(column_types_dict_list) != 1:
        raise ge_exceptions.ProfilerExecutionError(
            message="A unique column could not be found while obtaining semantic type information."
        )

    column_type: str = cast(str, column_types_dict_list[0][column_name]).upper()

    semantic_column_type: SemanticDomainTypes
    if column_type in [
        "UNIQUEIDENTIFIER",
    ]:
        semantic_column_type = SemanticDomainTypes.IDENTITY
    elif column_type in [
        "NUMERIC",
        "BINARY",
        "VARBINARY",
        "INT",
        "INT64",
        "INTEGER",
        "TINYINT",
        "SMALLINT",
        "BIGINT",
        "DECIMAL",
        "DOUBLE",
        "DOUBLE_PRECISION",
        "FLOAT",
        "FLOAT64",
        "REAL",
    ]:
        semantic_column_type = SemanticDomainTypes.NUMERIC
    elif column_type in [
        "TIME",
        "TIMESTAMP",
        "DATE",
        "DATETIME",
        "DATETIME2",
        "DATETIME64",
        "DATETIMEOFFSET",
        "SMALLDATETIME",
    ]:
        semantic_column_type = SemanticDomainTypes.DATETIME
    elif column_type in [
        "CHAR",
        "NCHAR",
        "NVARCHAR",
        "VARCHAR",
        "STRING",
        "TEXT",
        "NTEXT",
    ]:
        semantic_column_type = SemanticDomainTypes.TEXT
    elif column_type in [
        "BOOLEAN",
        "BIT",
    ]:
        semantic_column_type = SemanticDomainTypes.LOGIC
    elif column_type in [
        "MONEY",
        "SMALLMONEY",
    ]:
        semantic_column_type = SemanticDomainTypes.CURRENCY
    elif column_type in [
        "IMAGE",
    ]:
        semantic_column_type = SemanticDomainTypes.IMAGE
    elif column_type in [
        "SQL_VARIANT",
    ]:
        semantic_column_type = SemanticDomainTypes.MISCELLANEOUS
    else:
        semantic_column_type = SemanticDomainTypes.UNKNOWN

    return semantic_column_type
