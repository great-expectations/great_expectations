from typing import Dict, List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import ColumnDomainBuilder
from great_expectations.rule_based_profiler.types import (
    INFERRED_SEMANTIC_TYPE_KEY,
    Domain,
    ParameterContainer,
    SemanticDomainTypes,
)


class MultiColumnDomainBuilder(ColumnDomainBuilder):
    """
    This DomainBuilder uses relative tolerance of specified map metric to identify domains.
    """

    def __init__(
        self,
        include_column_names: Optional[Union[str, Optional[List[str]]]] = None,
        data_context: Optional["BaseDataContext"] = None,  # noqa: F821
    ) -> None:
        """
        Args:
            include_column_names: Explicitly specified desired columns
            data_context: BaseDataContext associated with this DomainBuilder
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
        return MetricDomainTypes.MULTICOLUMN

    def _get_domains(
        self,
        rule_name: str,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        """Return domains matching the specified tolerance limits.

        Args:
            rule_name: name of Rule object, for which "Domain" objects are obtained.
            variables: Optional variables to substitute when evaluating.

        Returns:
            List of domains that match the desired tolerance limits.
        """
        batch_ids: List[str] = self.get_batch_ids(variables=variables)

        validator: "Validator" = self.get_validator(variables=variables)  # noqa: F821

        effective_column_names: List[str] = self.get_effective_column_names(
            batch_ids=batch_ids,
            validator=validator,
            variables=variables,
        )

        if not (self.include_column_names and effective_column_names):
            raise ge_exceptions.ProfilerExecutionError(
                message=f'Error: "column_list" in {self.__class__.__name__} must not be empty.'
            )

        column_name: str
        semantic_types_by_column_name: Dict[str, SemanticDomainTypes] = {
            column_name: self.semantic_type_filter.table_column_name_to_inferred_semantic_domain_type_map[
                column_name
            ]
            for column_name in effective_column_names
        }

        domains: List[Domain] = [
            Domain(
                domain_type=self.domain_type,
                domain_kwargs={
                    "column_list": effective_column_names,
                },
                details={
                    INFERRED_SEMANTIC_TYPE_KEY: semantic_types_by_column_name,
                },
                rule_name=rule_name,
            ),
        ]

        return domains
