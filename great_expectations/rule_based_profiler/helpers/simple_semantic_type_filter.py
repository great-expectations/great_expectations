from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.domain import (
    InferredSemanticDomainType,
    SemanticDomainTypes,
)
from great_expectations.core.profiler_types_mapping import ProfilerTypeMapping
from great_expectations.rule_based_profiler.semantic_type_filter import (
    SemanticTypeFilter,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.validator.validator import Validator


class SimpleSemanticTypeFilter(SemanticTypeFilter):
    """
    This class provides default implementation methods, any of which can be overwritten with different mechanisms.
    """

    def __init__(
        self,
        batch_ids: Optional[List[str]] = None,
        validator: Optional[Validator] = None,
        column_names: Optional[List[str]] = None,
    ) -> None:
        self._build_table_column_name_to_inferred_semantic_domain_type_map(
            batch_ids=batch_ids,
            validator=validator,
            column_names=column_names,
        )

    @property
    def table_column_name_to_inferred_semantic_domain_type_map(
        self,
    ) -> Dict[str, SemanticDomainTypes]:
        return self._table_column_name_to_inferred_semantic_domain_type_map

    def parse_semantic_domain_type_argument(
        self,
        semantic_types: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ] = None,
    ) -> List[SemanticDomainTypes]:
        if semantic_types is None:
            return []

        semantic_type: Union[str, SemanticDomainTypes]
        if isinstance(semantic_types, str):
            semantic_types = semantic_types.lower()
            return [
                SemanticDomainTypes(semantic_type) for semantic_type in [semantic_types]
            ]

        if isinstance(semantic_types, SemanticDomainTypes):
            return [semantic_type for semantic_type in [semantic_types]]

        if isinstance(semantic_types, list):
            if all(isinstance(semantic_type, str) for semantic_type in semantic_types):
                semantic_types = [
                    semantic_type.lower() for semantic_type in semantic_types
                ]
                return [
                    SemanticDomainTypes(semantic_type)
                    for semantic_type in semantic_types
                ]

            if all(
                isinstance(semantic_type, SemanticDomainTypes)
                for semantic_type in semantic_types
            ):
                return [semantic_type for semantic_type in semantic_types]

            raise ValueError(
                "All elements in semantic_types list must be either of str or SemanticDomainTypes type."
            )

        raise ValueError("Unrecognized semantic_types directive.")

    def _build_table_column_name_to_inferred_semantic_domain_type_map(
        self,
        batch_ids: List[str],
        validator: Validator,
        column_names: List[str],
    ) -> None:
        column_types_dict_list: List[Dict[str, Any]] = validator.get_metric(
            metric=MetricConfiguration(
                metric_name="table.column_types",
                metric_domain_kwargs={
                    "batch_id": batch_ids[-1],  # active_batch_id
                },
                metric_value_kwargs={
                    "include_nested": True,
                },
            )
        )

        column_name: str
        self._table_column_name_to_inferred_semantic_domain_type_map = {
            column_name: self._infer_semantic_domain_type_from_table_column_type(
                column_types_dict_list=column_types_dict_list,
                column_name=column_name,
            ).semantic_domain_type
            for column_name in column_names
        }

    @staticmethod
    def _infer_semantic_domain_type_from_table_column_type(
        column_types_dict_list: List[Dict[str, Any]],
        column_name: str,
    ) -> InferredSemanticDomainType:
        # Note: As of Python 3.8, specifying argument type in Lambda functions is not supported by Lambda syntax.
        column_types_dict_list = list(
            filter(
                lambda column_type_dict: column_name == column_type_dict["name"],
                column_types_dict_list,
            )
        )
        if len(column_types_dict_list) != 1:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""Error: {len(column_types_dict_list)} columns were found while obtaining semantic type \
    information.  Please ensure that the specified column name refers to exactly one column.
    """
            )

        column_type: str = str(column_types_dict_list[0]["type"]).upper()

        semantic_column_type: SemanticDomainTypes
        if any(
            map(
                lambda type_name: column_type.startswith(type_name.upper()),
                ProfilerTypeMapping.INT_TYPE_NAMES
                + ProfilerTypeMapping.FLOAT_TYPE_NAMES,
            )
        ):
            semantic_column_type = SemanticDomainTypes.NUMERIC
        elif column_type in {
            type_name.upper() for type_name in ProfilerTypeMapping.STRING_TYPE_NAMES
        }:
            semantic_column_type = SemanticDomainTypes.TEXT
        elif column_type in {
            type_name.upper() for type_name in ProfilerTypeMapping.BOOLEAN_TYPE_NAMES
        }:
            semantic_column_type = SemanticDomainTypes.LOGIC
        elif any(
            map(
                lambda type_name: column_type.startswith(type_name.upper()),
                ProfilerTypeMapping.DATETIME_TYPE_NAMES,
            )
        ):
            semantic_column_type = SemanticDomainTypes.DATETIME
        elif column_type in {
            type_name.upper() for type_name in ProfilerTypeMapping.BINARY_TYPE_NAMES
        }:
            semantic_column_type = SemanticDomainTypes.BINARY
        elif column_type in {
            type_name.upper() for type_name in ProfilerTypeMapping.CURRENCY_TYPE_NAMES
        }:
            semantic_column_type = SemanticDomainTypes.CURRENCY
        elif column_type in {
            type_name.upper() for type_name in ProfilerTypeMapping.IDENTIFIER_TYPE_NAMES
        }:
            semantic_column_type = SemanticDomainTypes.IDENTIFIER
        elif column_type in (
            {
                type_name.upper()
                for type_name in ProfilerTypeMapping.MISCELLANEOUS_TYPE_NAMES
            }
            | {type_name.upper() for type_name in ProfilerTypeMapping.RECORD_TYPE_NAMES}
        ):
            semantic_column_type = SemanticDomainTypes.MISCELLANEOUS
        else:
            semantic_column_type = SemanticDomainTypes.UNKNOWN

        inferred_semantic_column_type: InferredSemanticDomainType = (
            InferredSemanticDomainType(
                semantic_domain_type=semantic_column_type,
                details={
                    "algorithm_type": "deterministic",
                    "mechanism": "lookup_table",
                    "source": "great_expectations.profile.base.ProfilerTypeMapping",
                },
            )
        )

        return inferred_semantic_column_type
