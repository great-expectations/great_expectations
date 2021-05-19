from typing import Any, Dict, List, Optional, Union, cast

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.domain_types import SemanticDomainTypes
from great_expectations.profile.base import ProfilerTypeMapping
from great_expectations.rule_based_profiler.domain_builder.column_domain_builder import (
    ColumnDomainBuilder,
)
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.rule_based_profiler.domain_builder.inferred_semantic_domain_type import (
    InferredSemanticDomainType,
)
from great_expectations.validator.validator import MetricConfiguration, Validator


class SimpleSemanticTypeColumnDomainBuilder(ColumnDomainBuilder):
    """
    This DomainBuilder utilizes a "best-effort" semantic interpretation of ("storage") columns of a table.
    """

    def __init__(
        self,
        semantic_types: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ] = None,
    ):
        if semantic_types is None:
            semantic_types = []
        self._semantic_types = semantic_types

    def _get_domains(
        self,
        *,
        validator: Optional[Validator] = None,
        batch_ids: Optional[List[str]] = None,
    ) -> List[Domain]:
        """
        Find the semantic column type for each column and return all domains matching the specified type or types.
        """
        if validator is None:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"{self.__class__.__name__} requires a reference to an instance of the Validator class."
            )

        semantic_types: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ] = _parse_semantic_domain_type_argument(semantic_types=self._semantic_types)

        table_column_names: List[str] = validator.get_metric(
            metric=MetricConfiguration(
                metric_name="table.columns",
                metric_domain_kwargs={},
                metric_value_kwargs=None,
                metric_dependencies=None,
            )
        )
        domains: List[Domain] = []
        column_name: str
        # A semantic type is distinguished from the structured column type;
        # An example structured column type would be "integer".  The inferred semantic type would be "id".
        inferred_semantic_domain_type: InferredSemanticDomainType
        semantic_domain_type: SemanticDomainTypes
        for column_name in table_column_names:
            inferred_semantic_domain_type = (
                self.infer_semantic_domain_type_from_table_column_type(
                    validator=validator, column_name=column_name
                )
            )
            semantic_domain_type = inferred_semantic_domain_type.semantic_domain_type
            # InferredSemanticDomainType also contains the "details" property capturing metadata about the inference.
            if semantic_domain_type in semantic_types:
                domains.append(
                    Domain(
                        domain_kwargs={
                            "column": column_name,
                            "batch_id": validator.active_batch_id,
                        },
                        domain_type=semantic_domain_type,
                    )
                )

        return domains

    # This method (default implementation) can be overwritten (with different implementation mechanisms) by subclasses.
    # noinspection PyMethodMayBeStatic
    def infer_semantic_domain_type_from_table_column_type(
        self, validator: Validator, column_name: str
    ) -> InferredSemanticDomainType:
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
            {type_name.upper() for type_name in ProfilerTypeMapping.INT_TYPE_NAMES}
            | {type_name.upper() for type_name in ProfilerTypeMapping.FLOAT_TYPE_NAMES}
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
        elif column_type in {
            type_name.upper() for type_name in ProfilerTypeMapping.DATETIME_TYPE_NAMES
        }:
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
            type_name.upper() for type_name in ProfilerTypeMapping.IDENTITY_TYPE_NAMES
        }:
            semantic_column_type = SemanticDomainTypes.IDENTITY
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


def _parse_semantic_domain_type_argument(
    semantic_types: Optional[
        Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
    ] = None
) -> List[SemanticDomainTypes]:
    if semantic_types is None:
        return []

    semantic_type: Union[str, SemanticDomainTypes]
    if isinstance(semantic_types, str):
        return [
            SemanticDomainTypes[semantic_type]
            for semantic_type in [semantic_types]
            if SemanticDomainTypes.has_member_key(key=semantic_type)
        ]
    if isinstance(semantic_types, SemanticDomainTypes):
        return [semantic_type for semantic_type in [semantic_types]]
    elif isinstance(semantic_types, List):
        if all([isinstance(semantic_type, str) for semantic_type in semantic_types]):
            return [
                SemanticDomainTypes[semantic_type]
                for semantic_type in semantic_types
                if SemanticDomainTypes.has_member_key(key=semantic_type)
            ]
        elif all(
            [
                isinstance(semantic_type, SemanticDomainTypes)
                for semantic_type in semantic_types
            ]
        ):
            return [semantic_type for semantic_type in semantic_types]
        else:
            raise ValueError(
                "All elements in semantic_types list must be either of str or SemanticDomainTypes type."
            )
    else:
        raise ValueError("Unrecognized semantic_types directive.")
