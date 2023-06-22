from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, List, Optional, Union

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.domain import (
    Domain,
    SemanticDomainTypes,
)
from great_expectations.rule_based_profiler.domain_builder import ColumnDomainBuilder
from great_expectations.rule_based_profiler.helpers.util import (
    build_domains_from_column_names,
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_container import (
    VARIABLES_KEY,
    ParameterContainer,
)
from great_expectations.util import is_candidate_subset_of_target
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.validator.validator import Validator


class DataProfilerColumnDomainBuilder(ColumnDomainBuilder):
    """
    This DomainBuilder emits "Domain" object for every column available in CapitalOne DataProfiler report.
    """

    def __init__(
        self,
        include_column_names: Optional[Union[str, Optional[List[str]]]] = None,
        exclude_column_names: Optional[Union[str, Optional[List[str]]]] = None,
        include_column_name_suffixes: Optional[Union[str, Iterable, List[str]]] = None,
        exclude_column_name_suffixes: Optional[Union[str, Iterable, List[str]]] = None,
        semantic_type_filter_module_name: Optional[str] = None,
        semantic_type_filter_class_name: Optional[str] = None,
        include_semantic_types: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ] = None,
        exclude_semantic_types: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ] = None,
        data_context: Optional[AbstractDataContext] = None,
    ) -> None:
        """
        Args:
            profile_path: path to output (in ".pkl" format) of CapitalOne DataProfiler (default references "variables").
            include_column_names: Explicitly specified desired columns (if None, it is computed based on active Batch).
            exclude_column_names: If provided, these columns are pre-filtered and excluded from consideration.
            include_column_name_suffixes: Explicitly specified desired suffixes for corresponding columns to match.
            exclude_column_name_suffixes: Explicitly specified desired suffixes for corresponding columns to not match.
            semantic_type_filter_module_name: module_name containing class that implements SemanticTypeFilter interfaces
            semantic_type_filter_class_name: class_name of class that implements SemanticTypeFilter interfaces
            include_semantic_types: single/multiple type specifications using SemanticDomainTypes (or str equivalents)
            to be included
            exclude_semantic_types: single/multiple type specifications using SemanticDomainTypes (or str equivalents)
            to be excluded
            data_context: AbstractDataContext associated with this DomainBuilder
        """
        super().__init__(
            include_column_names=include_column_names,
            exclude_column_names=exclude_column_names,
            include_column_name_suffixes=include_column_name_suffixes,
            exclude_column_name_suffixes=exclude_column_name_suffixes,
            semantic_type_filter_module_name=semantic_type_filter_module_name,
            semantic_type_filter_class_name=semantic_type_filter_class_name,
            include_semantic_types=include_semantic_types,
            exclude_semantic_types=exclude_semantic_types,
            data_context=data_context,
        )

    def _get_domains(
        self,
        rule_name: str,
        variables: Optional[ParameterContainer] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> List[Domain]:
        """Return domains matching the specified tolerance limits.

        Args:
            rule_name: name of Rule object, for which "Domain" objects are obtained.
            variables: Optional variables to substitute when evaluating.
            runtime_configuration: Additional run-time settings (see "Validator.DEFAULT_RUNTIME_CONFIGURATION").

        Returns:
            List of domains that match the desired tolerance limits.
        """
        batch_ids: List[str] = self.get_batch_ids(variables=variables)  # type: ignore[assignment] # could be None

        validator: Validator = self.get_validator(variables=variables)  # type: ignore[assignment] # could be None

        table_column_names: List[str] = self.get_table_column_names(
            batch_ids=batch_ids,
            validator=validator,
            variables=variables,
        )

        # Obtain profile_path from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        profile_path: str = get_parameter_value_and_validate_return_type(
            domain=None,
            parameter_reference=f"{VARIABLES_KEY}profile_path",
            expected_return_type=str,
            variables=variables,
            parameters=None,
        )

        profile_report_filtering_key: str = (
            get_parameter_value_and_validate_return_type(
                domain=None,
                parameter_reference=f"{VARIABLES_KEY}profile_report_filtering_key",
                expected_return_type=str,
                variables=variables,
                parameters=None,
            )
        )

        profile_report_accepted_filtering_values: str = get_parameter_value_and_validate_return_type(
            domain=None,
            parameter_reference=f"{VARIABLES_KEY}profile_report_accepted_filtering_values",
            expected_return_type=list,
            variables=variables,
            parameters=None,
        )

        # get metrics and profile path from variables and then pass them into here
        profile_report_column_names: List[str] = validator.get_metric(  # type: ignore[assignment] # could be None
            metric=MetricConfiguration(
                metric_name="data_profiler.table_column_list",
                metric_domain_kwargs={},
                metric_value_kwargs={
                    "profile_path": profile_path,
                    "profile_report_filtering_key": profile_report_filtering_key,
                    "profile_report_accepted_filtering_values": profile_report_accepted_filtering_values,
                },
            )
        )

        if not (profile_report_column_names and table_column_names):
            raise gx_exceptions.ProfilerExecutionError(
                message=f"Error: List of available table columns in {self.__class__.__name__} must not be empty."
            )

        if not is_candidate_subset_of_target(
            candidate=profile_report_column_names, target=table_column_names
        ):
            raise gx_exceptions.ProfilerExecutionError(
                message=f"Error: Some of profiled columns in {self.__class__.__name__} are not found in Batch table."
            )

        effective_column_names: List[str] = self.get_filtered_column_names(
            column_names=profile_report_column_names,
            batch_ids=batch_ids,
            validator=validator,
            variables=variables,
        )

        domains: List[Domain] = build_domains_from_column_names(
            rule_name=rule_name,
            column_names=effective_column_names,
            domain_type=self.domain_type,
            table_column_name_to_inferred_semantic_domain_type_map=self.semantic_type_filter.table_column_name_to_inferred_semantic_domain_type_map,  # type: ignore[union-attr] # could be None
        )

        return domains
