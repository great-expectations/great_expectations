from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import ColumnDomainBuilder
from great_expectations.rule_based_profiler.helpers.cardinality_checker import (
    AbsoluteCardinalityLimit,
    CardinalityChecker,
    CardinalityLimitMode,
    RelativeCardinalityLimit,
    validate_input_parameters,
)
from great_expectations.rule_based_profiler.helpers.util import (
    build_simple_domains_from_column_names,
    get_parameter_value_and_validate_return_type,
    get_resolved_metrics_by_key,
)
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterContainer,
    SemanticDomainTypes,
)
from great_expectations.validator.metric_configuration import MetricConfiguration


class CategoricalColumnDomainBuilder(ColumnDomainBuilder):
    """
    This DomainBuilder uses column cardinality to identify domains.
    """

    exclude_field_names: Set[str] = ColumnDomainBuilder.exclude_field_names | {
        "cardinality_checker",
    }

    def __init__(
        self,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
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
        allowed_semantic_types_passthrough: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ] = None,
        limit_mode: Optional[Union[CardinalityLimitMode, str]] = None,
        max_unique_values: Optional[Union[str, int]] = None,
        max_proportion_unique: Optional[Union[str, float]] = None,
    ):
        """Create column domains where cardinality is within the specified limit.

        Cardinality refers to the number of unique values in a given domain.
        Categorical generally refers to columns with relatively limited
        number of unique values.
        Limit mode can be absolute (number of unique values) or relative
        (proportion of unique values). You can choose one of: limit_mode,
        max_unique_values or max_proportion_unique to specify the cardinality
        limit.
        Note that the limit must be met for each batch separately that is
        supplied in the batch_request or the column domain will not be included.
        Note that the columns used will be from the first batch retrieved
        via the batch_request. If other batches contain additional columns,
        these will not be considered.

        Args:
            batch_list: explicitly specified Batch objects for use in DomainBuilder
            batch_request: BatchRequest to be optionally used to define batches to consider for this domain builder.
            data_context: DataContext associated with this profiler.
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
            allowed_semantic_types_passthrough: single/multiple type specifications using SemanticDomainTypes
            (or str equivalents) to be allowed without processing, if encountered among available column_names
            limit_mode: CardinalityLimitMode or string name of the mode
                defining the maximum allowable cardinality to use when
                filtering columns.
            max_unique_values: number of max unique rows for a custom
                cardinality limit to use when filtering columns.
            max_proportion_unique: proportion of unique values for a
                custom cardinality limit to use when filtering columns.
        """
        if exclude_column_names is None:
            exclude_column_names = [
                "id",
            ]

        if exclude_semantic_types is None:
            exclude_semantic_types = [
                SemanticDomainTypes.BINARY,
                SemanticDomainTypes.CURRENCY,
                SemanticDomainTypes.IDENTIFIER,
            ]

        if allowed_semantic_types_passthrough is None:
            allowed_semantic_types_passthrough = [
                SemanticDomainTypes.LOGIC,
            ]

        self._allowed_semantic_types_passthrough = allowed_semantic_types_passthrough

        super().__init__(
            batch_list=batch_list,
            batch_request=batch_request,
            data_context=data_context,
            include_column_names=include_column_names,
            exclude_column_names=exclude_column_names,
            include_column_name_suffixes=include_column_name_suffixes,
            exclude_column_name_suffixes=exclude_column_name_suffixes,
            semantic_type_filter_module_name=semantic_type_filter_module_name,
            semantic_type_filter_class_name=semantic_type_filter_class_name,
            include_semantic_types=include_semantic_types,
            exclude_semantic_types=exclude_semantic_types,
        )

        self._limit_mode = limit_mode
        self._max_unique_values = max_unique_values
        self._max_proportion_unique = max_proportion_unique

        self._cardinality_checker = None

    @property
    def domain_type(self) -> Union[str, MetricDomainTypes]:
        return MetricDomainTypes.COLUMN

    @property
    def allowed_semantic_types_passthrough(
        self,
    ) -> Optional[
        Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
    ]:
        return self._allowed_semantic_types_passthrough

    @allowed_semantic_types_passthrough.setter
    def allowed_semantic_types_passthrough(
        self,
        value: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ],
    ):
        self._allowed_semantic_types_passthrough = value

    @property
    def limit_mode(self) -> Optional[Union[CardinalityLimitMode, str]]:
        return self._limit_mode

    @property
    def max_unique_values(self) -> Optional[Union[str, int]]:
        return self._max_unique_values

    @property
    def max_proportion_unique(self) -> Optional[Union[str, float]]:
        return self._max_proportion_unique

    @property
    def cardinality_checker(self) -> Optional[CardinalityChecker]:
        return self._cardinality_checker

    def _get_domains(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        """Return domains matching the selected limit_mode.

        Args:
            variables: Optional variables to substitute when evaluating.

        Returns:
            List of domains that match the desired cardinality.
        """
        batch_ids: List[str] = self.get_batch_ids(variables=variables)

        validator: "Validator" = self.get_validator(variables=variables)  # noqa: F821

        effective_column_names: List[str] = self.get_effective_column_names(
            batch_ids=batch_ids,
            validator=validator,
            variables=variables,
        )

        # Obtain limit_mode from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        limit_mode: Optional[
            Union[CardinalityLimitMode, str]
        ] = get_parameter_value_and_validate_return_type(
            domain=None,
            parameter_reference=self.limit_mode,
            expected_return_type=None,
            variables=variables,
            parameters=None,
        )

        # Obtain max_unique_values from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        max_unique_values: Optional[int] = get_parameter_value_and_validate_return_type(
            domain=None,
            parameter_reference=self.max_unique_values,
            expected_return_type=None,
            variables=variables,
            parameters=None,
        )

        # Obtain max_proportion_unique from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        max_proportion_unique: Optional[
            float
        ] = get_parameter_value_and_validate_return_type(
            domain=None,
            parameter_reference=self.max_proportion_unique,
            expected_return_type=None,
            variables=variables,
            parameters=None,
        )

        validate_input_parameters(
            limit_mode=limit_mode,
            max_unique_values=max_unique_values,
            max_proportion_unique=max_proportion_unique,
        )

        self._cardinality_checker = CardinalityChecker(
            limit_mode=limit_mode,
            max_unique_values=max_unique_values,
            max_proportion_unique=max_proportion_unique,
        )

        # Obtain allowed_semantic_types_passthrough from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        allowed_semantic_types_passthrough: Union[
            str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]
        ] = get_parameter_value_and_validate_return_type(
            domain=None,
            parameter_reference=self.allowed_semantic_types_passthrough,
            expected_return_type=None,
            variables=variables,
            parameters=None,
        )
        allowed_semantic_types_passthrough = (
            self.semantic_type_filter.parse_semantic_domain_type_argument(
                semantic_types=allowed_semantic_types_passthrough
            )
        )

        column_name: str

        allowed_column_names_passthrough: List[str] = [
            column_name
            for column_name in effective_column_names
            if self.semantic_type_filter.table_column_name_to_inferred_semantic_domain_type_mapping[
                column_name
            ]
            in allowed_semantic_types_passthrough
        ]

        effective_column_names = [
            column_name
            for column_name in effective_column_names
            if column_name not in allowed_column_names_passthrough
        ]

        metrics_for_cardinality_check: Dict[
            str, List[MetricConfiguration]
        ] = self._generate_metric_configurations_to_check_cardinality(
            batch_ids=batch_ids, column_names=effective_column_names
        )

        candidate_column_names: List[
            str
        ] = self._column_names_meeting_cardinality_limit(
            validator=validator,
            metrics_for_cardinality_check=metrics_for_cardinality_check,
        )
        candidate_column_names.extend(allowed_column_names_passthrough)

        return build_simple_domains_from_column_names(
            column_names=candidate_column_names,
            domain_type=self.domain_type,
        )

    def _generate_metric_configurations_to_check_cardinality(
        self,
        batch_ids: List[str],
        column_names: List[str],
    ) -> Dict[str, List[MetricConfiguration]]:
        """Generate metric configurations used to compute metrics for checking cardinality.

        Args:
            batch_ids: List of batch_ids used to create metric configurations.
            column_names: List of column_names used to create metric configurations.

        Returns:
            Dictionary of the form {
                "my_column_name": List[MetricConfiguration],
            }
        """

        limit_mode: Union[
            AbsoluteCardinalityLimit, RelativeCardinalityLimit
        ] = self.cardinality_checker.limit_mode

        batch_id: str
        metric_configurations: Dict[str, List[MetricConfiguration]] = {
            column_name: [
                MetricConfiguration(
                    metric_name=limit_mode.metric_name_defining_limit,
                    metric_domain_kwargs={
                        "column": column_name,
                        "batch_id": batch_id,
                    },
                    metric_value_kwargs=None,
                    metric_dependencies=None,
                )
                for batch_id in batch_ids
            ]
            for column_name in column_names
        }

        return metric_configurations

    def _column_names_meeting_cardinality_limit(
        self,
        validator: "Validator",  # noqa: F821
        metrics_for_cardinality_check: Dict[str, List[MetricConfiguration]],
    ) -> List[str]:
        """Compute cardinality and return column names meeting cardinality limit.

        Args:
            validator: Validator used to compute column cardinality.
            metrics_for_cardinality_check: metric configurations used to compute cardinality.

        Returns:
            List of column names meeting cardinality.
        """
        column_name: str
        resolved_metrics: Dict[Tuple[str, str, str], Any]
        metric_value: Any

        resolved_metrics_by_column_name: Dict[
            str, Dict[Tuple[str, str, str], Any]
        ] = get_resolved_metrics_by_key(
            validator=validator,
            metric_configurations_by_key=metrics_for_cardinality_check,
        )

        candidate_column_names: List[str] = [
            column_name
            for column_name, resolved_metrics in resolved_metrics_by_column_name.items()
            if all(
                [
                    self.cardinality_checker.cardinality_within_limit(
                        metric_value=metric_value
                    )
                    for metric_value in resolved_metrics.values()
                ]
            )
        ]

        return candidate_column_names
