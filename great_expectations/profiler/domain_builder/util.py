from typing import Any, Dict, List, cast

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.domain_types import SemanticDomainTypes
from great_expectations.profile.base import ProfilerTypeMapping
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
            message=f"""Error: {len(column_types_dict_list)} columns were found while obtaining semantic type \
information.  Please ensure that the specified column name refers to exactly one column.
"""
        )

    column_type: str = cast(str, column_types_dict_list[0][column_name]).upper()

    semantic_column_type: SemanticDomainTypes
    if column_type in (
        set([type_name.upper() for type_name in ProfilerTypeMapping.INT_TYPE_NAMES])
        | set([type_name.upper() for type_name in ProfilerTypeMapping.FLOAT_TYPE_NAMES])
    ):
        semantic_column_type = SemanticDomainTypes.NUMERIC
    elif column_type in set(
        [type_name.upper() for type_name in ProfilerTypeMapping.STRING_TYPE_NAMES]
    ):
        semantic_column_type = SemanticDomainTypes.TEXT
    elif column_type in set(
        [type_name.upper() for type_name in ProfilerTypeMapping.BOOLEAN_TYPE_NAMES]
    ):
        semantic_column_type = SemanticDomainTypes.LOGIC
    elif column_type in set(
        [type_name.upper() for type_name in ProfilerTypeMapping.DATETIME_TYPE_NAMES]
    ):
        semantic_column_type = SemanticDomainTypes.DATETIME
    elif column_type in set(
        [type_name.upper() for type_name in ProfilerTypeMapping.BINARY_TYPE_NAMES]
    ):
        semantic_column_type = SemanticDomainTypes.BINARY
    elif column_type in set(
        [type_name.upper() for type_name in ProfilerTypeMapping.CURRENCY_TYPE_NAMES]
    ):
        semantic_column_type = SemanticDomainTypes.CURRENCY
    elif column_type in set(
        [type_name.upper() for type_name in ProfilerTypeMapping.IDENTITY_TYPE_NAMES]
    ):
        semantic_column_type = SemanticDomainTypes.IDENTITY
    elif column_type in (
        set(
            [
                type_name.upper()
                for type_name in ProfilerTypeMapping.MISCELLANEOUS_TYPE_NAMES
            ]
        )
        | set(
            [type_name.upper() for type_name in ProfilerTypeMapping.RECORD_TYPE_NAMES]
        )
    ):
        semantic_column_type = SemanticDomainTypes.MISCELLANEOUS
    else:
        semantic_column_type = SemanticDomainTypes.UNKNOWN

    return semantic_column_type
