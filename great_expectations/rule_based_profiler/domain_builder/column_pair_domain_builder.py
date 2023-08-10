from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Dict, List, Optional, Set, Union

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.domain import (
    INFERRED_SEMANTIC_TYPE_KEY,
    Domain,
    SemanticDomainTypes,
)
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import ColumnDomainBuilder
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,  # noqa: TCH001
)

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.validator.validator import Validator


class ColumnPairDomainBuilder(ColumnDomainBuilder):
    """
    This DomainBuilder uses "include_column_names" property of its parent class to specify "column_A" and "column_B" (order-preserving).
    """

    exclude_field_names: ClassVar[
        Set[str]
    ] = ColumnDomainBuilder.exclude_field_names | {
        "exclude_column_names",
        "include_column_name_suffixes",
        "exclude_column_name_suffixes",
        "semantic_type_filter_module_name",
        "semantic_type_filter_class_name",
        "include_semantic_types",
        "exclude_semantic_types",
    }

    def __init__(
        self,
        include_column_names: Optional[Union[str, Optional[List[str]]]] = None,
        data_context: Optional[AbstractDataContext] = None,
    ) -> None:
        """
        Args:
            include_column_names: Explicitly specified exactly two desired columns
            data_context: AbstractDataContext associated with this DomainBuilder
        """
        super().__init__(
            include_column_names=include_column_names,
            exclude_column_names=None,
            include_column_name_suffixes=None,
            exclude_column_name_suffixes=None,
            semantic_type_filter_module_name=None,
            semantic_type_filter_class_name=None,
            include_semantic_types=None,
            exclude_semantic_types=None,
            data_context=data_context,
        )

    @property
    def domain_type(self) -> MetricDomainTypes:
        return MetricDomainTypes.COLUMN_PAIR

    def _get_domains(
        self,
        rule_name: str,
        variables: Optional[ParameterContainer] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> List[Domain]:
        """Obtains and returns Domain object, whose domain_kwargs consists of "column_A" and "column_B" (order-preserving) column-pair.

        Args:
            rule_name: name of Rule object, for which "Domain" objects are obtained.
            variables: Optional variables to substitute when evaluating.
            runtime_configuration: Optional[dict] = None,

        Returns:
            List of domains that match the desired tolerance limits.
        """
        batch_ids: List[str] = self.get_batch_ids(variables=variables)  # type: ignore[assignment] # could be None

        validator: Validator = self.get_validator(variables=variables)  # type: ignore[assignment] # could be None

        effective_column_names: List[str] = self.get_effective_column_names(
            batch_ids=batch_ids,
            validator=validator,
            variables=variables,
        )

        if not (
            effective_column_names
            and (len(effective_column_names) == 2)  # noqa: PLR2004
        ):
            raise gx_exceptions.ProfilerExecutionError(
                message=f"""Error: Columns specified for {self.__class__.__name__} in sorted order must correspond to \
"column_A" and "column_B" (in this exact order).
"""
            )

        domain_kwargs: Dict[str, str] = dict(
            zip(
                [
                    "column_A",
                    "column_B",
                ],
                effective_column_names,
            )
        )

        column_name: str
        semantic_types_by_column_name: Dict[str, SemanticDomainTypes] = {
            column_name: self.semantic_type_filter.table_column_name_to_inferred_semantic_domain_type_map[  # type: ignore[union-attr] # could be None
                column_name
            ]
            for column_name in effective_column_names
        }

        domains: List[Domain] = [
            Domain(
                domain_type=self.domain_type,
                domain_kwargs=domain_kwargs,
                details={
                    INFERRED_SEMANTIC_TYPE_KEY: semantic_types_by_column_name,
                },
                rule_name=rule_name,
            ),
        ]

        return domains
