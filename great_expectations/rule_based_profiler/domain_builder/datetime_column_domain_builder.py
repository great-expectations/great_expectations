# from __future__ import annotations
#
# from typing import (
#     TYPE_CHECKING,
#     ClassVar,
#     Dict,
#     Iterable,
#     List,
#     Optional,
#     Set,
#     Tuple,
#     Type,
#     Union,
# )
#
# import great_expectations.exceptions as gx_exceptions
# from great_expectations.core.domain import Domain, SemanticDomainTypes
# from great_expectations.core.metric_domain_types import MetricDomainTypes
# from great_expectations.rule_based_profiler.domain_builder import ColumnDomainBuilder
# from great_expectations.rule_based_profiler.helpers.util import (
#     build_domains_from_column_names,
#     get_parameter_value_and_validate_return_type,
#     get_resolved_metrics_by_key,
# )
# from great_expectations.validator.metric_configuration import MetricConfiguration
#
# if TYPE_CHECKING:
#     from great_expectations.data_context.data_context.abstract_data_context import (
#         AbstractDataContext,
#     )
#     from great_expectations.rule_based_profiler.parameter_container import (
#         ParameterContainer,
#     )
#     from great_expectations.validator.computed_metric import MetricValue
#     from great_expectations.validator.validator import Validator
#
#
# class CategoricalColumnDomainBuilder(ColumnDomainBuilder):
#     """
#     This DomainBuilder uses sementic information and data inspection to identify datetime domains.
#     """
#
#     def __init__(
#         self,
#         include_column_names: Optional[Union[str, Optional[List[str]]]] = None,
#         exclude_column_names: Optional[Union[str, Optional[List[str]]]] = None,
#         include_column_name_suffixes: Optional[Union[str, Iterable, List[str]]] = None,
#         exclude_column_name_suffixes: Optional[Union[str, Iterable, List[str]]] = None,
#         semantic_type_filter_module_name: Optional[str] = None,
#         semantic_type_filter_class_name: Optional[str] = None,
#         include_semantic_types: Optional[
#             Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
#         ] = None,
#         exclude_semantic_types: Optional[
#             Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
#         ] = None,
#         data_context: Optional[AbstractDataContext] = None,
#     ) -> None:
#         """Create column domains where column values have datetime interpretation.
#
#         Args:
#             include_column_names: Explicitly specified desired columns (if None, it is computed based on active Batch).
#             exclude_column_names: If provided, these columns are pre-filtered and excluded from consideration.
#             include_column_name_suffixes: Explicitly specified desired suffixes for corresponding columns to match.
#             exclude_column_name_suffixes: Explicitly specified desired suffixes for corresponding columns to not match.
#             semantic_type_filter_module_name: module_name containing class that implements SemanticTypeFilter interfaces
#             semantic_type_filter_class_name: class_name of class that implements SemanticTypeFilter interfaces
#             include_semantic_types: single/multiple type specifications using SemanticDomainTypes (or str equivalents)
#             to be included
#             exclude_semantic_types: single/multiple type specifications using SemanticDomainTypes (or str equivalents)
#             to be excluded
#             data_context: AbstractDataContext associated with this DomainBuilder
#         """
#         if exclude_column_names is None:
#             exclude_column_names = [
#                 "id",
#                 "ID",
#                 "Id",
#             ]
#
#         if exclude_column_name_suffixes is None:
#             exclude_column_name_suffixes = [
#                 "_id",
#                 "_ID",
#             ]
#
#         if exclude_semantic_types is None:
#             exclude_semantic_types = [
#                 SemanticDomainTypes.BINARY,
#                 SemanticDomainTypes.CURRENCY,
#                 SemanticDomainTypes.IDENTIFIER,
#             ]
#
#         super().__init__(
#             include_column_names=include_column_names,
#             exclude_column_names=exclude_column_names,
#             include_column_name_suffixes=include_column_name_suffixes,
#             exclude_column_name_suffixes=exclude_column_name_suffixes,
#             semantic_type_filter_module_name=semantic_type_filter_module_name,
#             semantic_type_filter_class_name=semantic_type_filter_class_name,
#             include_semantic_types=include_semantic_types,
#             exclude_semantic_types=exclude_semantic_types,
#             data_context=data_context,
#         )
#
#     @property
#     def domain_type(self) -> MetricDomainTypes:
#         return MetricDomainTypes.COLUMN
#
#     def _get_domains(
#         self,
#         rule_name: str,
#         variables: Optional[ParameterContainer] = None,
#         runtime_configuration: Optional[dict] = None,
#     ) -> List[Domain]:
#         """Return domains matching datetime interpretation.
#
#         Args:
#             rule_name: name of Rule object, for which "Domain" objects are obtained.
#             variables: Optional variables to substitute when evaluating.
#             runtime_configuration: Additional run-time settings (see "Validator.DEFAULT_RUNTIME_CONFIGURATION").
#
#         Returns:
#             List of domains whose constituent column attributes have datetime interpretation.
#         """
#         batch_ids: Optional[List[str]] = self.get_batch_ids(variables=variables)
#
#         validator: Optional[Validator] = self.get_validator(variables=variables)
#
#         effective_column_names: List[str] = self.get_effective_column_names(
#             batch_ids=batch_ids,
#             validator=validator,
#             variables=variables,
#         )
#
#         column_name: str
#
#         allowed_column_names_passthrough: List[str] = [
#             column_name
#             for column_name in effective_column_names
#             if self.semantic_type_filter.table_column_name_to_inferred_semantic_domain_type_map[  # type: ignore[operator,union-attr] # could be None
#                 column_name
#             ]
#             in allowed_semantic_types_passthrough
#         ]
#
#         effective_column_names = [
#             column_name
#             for column_name in effective_column_names
#             if column_name not in allowed_column_names_passthrough
#         ]
#
#         metrics_for_cardinality_check: Dict[
#             str, List[MetricConfiguration]
#         ] = self._generate_metric_configurations_to_check_cardinality(
#             column_names=effective_column_names, batch_ids=batch_ids
#         )
#
#         if validator is None:
#             raise gx_exceptions.ProfilerExecutionError(
#                 message=f"Error: Failed to obtain Validator {self.__class__.__name__} (Validator is required for cardinality checks)."
#             )
#
#         candidate_column_names: List[
#             str
#         ] = self._column_names_meeting_cardinality_limit(
#             validator=validator,
#             metrics_for_cardinality_check=metrics_for_cardinality_check,
#             runtime_configuration=runtime_configuration,
#         )
#         candidate_column_names.extend(allowed_column_names_passthrough)
#
#         domains: List[Domain] = build_domains_from_column_names(
#             rule_name=rule_name,
#             column_names=candidate_column_names,
#             domain_type=self.domain_type,
#             table_column_name_to_inferred_semantic_domain_type_map=self.semantic_type_filter.table_column_name_to_inferred_semantic_domain_type_map,  # type: ignore[union-attr] # could be None
#         )
#
#         return domains
#
#     def _generate_metric_configurations_to_check_cardinality(
#         self,
#         column_names: List[str],
#         batch_ids: Optional[List[str]] = None,
#     ) -> Dict[str, List[MetricConfiguration]]:
#         """Generate metric configurations used to compute metrics for checking cardinality.
#
#         Args:
#             column_names: List of column_names used to create metric configurations.
#             batch_ids: List of batch_ids used to create metric configurations.
#
#         Returns:
#             Dictionary of the form {
#                 "my_column_name": List[MetricConfiguration],
#             }
#         """
#         batch_ids = batch_ids or []
#
#         cardinality_limit_mode: Union[
#             AbsoluteCardinalityLimit, RelativeCardinalityLimit
#         ] = self.cardinality_checker.cardinality_limit_mode  # type: ignore[union-attr] # could be None
#
#         batch_id: str
#         metric_configurations: Dict[str, List[MetricConfiguration]] = {
#             column_name: [
#                 MetricConfiguration(
#                     metric_name=cardinality_limit_mode.metric_name_defining_limit,
#                     metric_domain_kwargs={
#                         "column": column_name,
#                         "batch_id": batch_id,
#                     },
#                     metric_value_kwargs=None,
#                 )
#                 for batch_id in batch_ids
#             ]
#             for column_name in column_names
#         }
#
#         return metric_configurations
#
#     def _column_names_meeting_cardinality_limit(
#         self,
#         validator: Validator,
#         metrics_for_cardinality_check: Dict[str, List[MetricConfiguration]],
#         runtime_configuration: Optional[dict] = None,
#     ) -> List[str]:
#         """Compute cardinality and return column names meeting cardinality limit.
#
#         Args:
#             validator: Validator used to compute column cardinality.
#             metrics_for_cardinality_check: metric configurations used to compute cardinality.
#             runtime_configuration: Additional run-time settings (see "Validator.DEFAULT_RUNTIME_CONFIGURATION").
#
#         Returns:
#             List of column names meeting cardinality.
#         """
#         column_name: str
#         resolved_metrics: Dict[Tuple[str, str, str], MetricValue]
#         metric_value: MetricValue
#
#         resolved_metrics_by_column_name: Dict[
#             str, Dict[Tuple[str, str, str], MetricValue]
#         ] = get_resolved_metrics_by_key(
#             validator=validator,
#             metric_configurations_by_key=metrics_for_cardinality_check,
#             runtime_configuration=runtime_configuration,
#         )
#
#         candidate_column_names: List[str] = [
#             column_name
#             for column_name, resolved_metrics in resolved_metrics_by_column_name.items()
#             if all(
#                 self.cardinality_checker.cardinality_within_limit(  # type: ignore[union-attr] # could be None
#                     metric_value=metric_value  # type: ignore[arg-type] # Expecting Union[int, float] (subset of "MetricValue").
#                 )
#                 for metric_value in resolved_metrics.values()
#             )
#         ]
#
#         return candidate_column_names
