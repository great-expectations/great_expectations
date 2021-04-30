from typing import Any, Dict, List, cast

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.domain_types import SemanticDomainTypes
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator


def translate_table_column_type_to_semantic_domain_type(
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
