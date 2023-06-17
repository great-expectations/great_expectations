from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    ClassVar,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.domain import Domain, SemanticDomainTypes
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import ColumnDomainBuilder
from great_expectations.rule_based_profiler.helpers.cardinality_checker import (
    AbsoluteCardinalityLimit,
    CardinalityChecker,
    CardinalityLimitMode,
    RelativeCardinalityLimit,
    validate_input_parameters,
)
from great_expectations.rule_based_profiler.helpers.util import (
    build_domains_from_column_names,
    get_parameter_value_and_validate_return_type,
    get_resolved_metrics_by_key,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.rule_based_profiler.parameter_container import (
        ParameterContainer,
    )
    from great_expectations.validator.computed_metric import MetricValue
    from great_expectations.validator.validator import Validator


class CategoricalColumnDomainBuilder(ColumnDomainBuilder):
    """
    This DomainBuilder uses column cardinality to identify domains.
    """

    exclude_field_names: ClassVar[
        Set[str]
    ] = ColumnDomainBuilder.exclude_field_names | {
        "cardinality_checker",
    }
    cardinality_limit_modes: Type[CardinalityLimitMode] = CardinalityLimitMode

    def __init__(  # noqa: PLR0913
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
        allowed_semantic_types_passthrough: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ] = None,
        cardinality_limit_mode: Optional[Union[str, CardinalityLimitMode, dict]] = None,
        max_unique_values: Optional[Union[str, int]] = None,
        max_proportion_unique: Optional[Union[str, float]] = None,
        data_context: Optional[AbstractDataContext] = None,
    ) -> None:
        """Create column domains where cardinality is within the specified limit.

        Cardinality refers to the number of unique values in a given domain.
        Categorical generally refers to columns with relatively limited
        number of unique values.
        Limit mode can be absolute (number of unique values) or relative
        (proportion of unique values). You can choose one of: cardinality_limit_mode,
        max_unique_values or max_proportion_unique to specify the cardinality
        limit.
        Note that the limit must be met for each Batch separately.
        If other Batch objects contain additional columns, these will not be considered.

        Args:
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
            cardinality_limit_mode: CardinalityLimitMode or string name of the mode
                defining the maximum allowable cardinality to use when
                filtering columns.
                Accessible for convenience via CategoricalColumnDomainBuilder.cardinality_limit_modes e.g.:
                cardinality_limit_mode=CategoricalColumnDomainBuilder.cardinality_limit_modes.VERY_FEW,
            max_unique_values: number of max unique rows for a custom
                cardinality limit to use when filtering columns.
            max_proportion_unique: proportion of unique values for a
                custom cardinality limit to use when filtering columns.
            data_context: AbstractDataContext associated with this DomainBuilder
        """
        if exclude_column_names is None:
            exclude_column_names = [
                "id",
                "ID",
                "Id",
            ]

        if exclude_column_name_suffixes is None:
            exclude_column_name_suffixes = [
                "_id",
                "_ID",
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

        self._allowed_semantic_types_passthrough: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ] = allowed_semantic_types_passthrough

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

        self._cardinality_limit_mode = cardinality_limit_mode
        self._max_unique_values = max_unique_values
        self._max_proportion_unique = max_proportion_unique

        self._cardinality_checker: Optional[CardinalityChecker] = None

    @property
    def domain_type(self) -> MetricDomainTypes:
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
    ) -> None:
        self._allowed_semantic_types_passthrough = value

    @property
    def cardinality_limit_mode(
        self,
    ) -> Optional[Union[str, CardinalityLimitMode, dict]]:
        return self._cardinality_limit_mode

    @cardinality_limit_mode.setter
    def cardinality_limit_mode(
        self, value: Optional[Union[str, CardinalityLimitMode, dict]]
    ) -> None:
        self._cardinality_limit_mode = value

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
        rule_name: str,
        variables: Optional[ParameterContainer] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> List[Domain]:
        """Return domains matching the selected cardinality_limit_mode.

        Args:
            rule_name: name of Rule object, for which "Domain" objects are obtained.
            variables: Optional variables to substitute when evaluating.
            runtime_configuration: Additional run-time settings (see "Validator.DEFAULT_RUNTIME_CONFIGURATION").

        Returns:
            List of domains that match the desired cardinality.
        """
        batch_ids: Optional[List[str]] = self.get_batch_ids(variables=variables)

        validator: Optional[Validator] = self.get_validator(variables=variables)

        effective_column_names: List[str] = self.get_effective_column_names(
            batch_ids=batch_ids,
            validator=validator,
            variables=variables,
        )

        # Obtain cardinality_limit_mode from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        cardinality_limit_mode: Optional[
            Union[str, CardinalityLimitMode, dict]
        ] = get_parameter_value_and_validate_return_type(
            domain=None,
            parameter_reference=self.cardinality_limit_mode,
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
            cardinality_limit_mode=cardinality_limit_mode,
            max_unique_values=max_unique_values,
            max_proportion_unique=max_proportion_unique,
        )

        self._cardinality_checker = CardinalityChecker(
            cardinality_limit_mode=cardinality_limit_mode,
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
        allowed_semantic_types_passthrough = self.semantic_type_filter.parse_semantic_domain_type_argument(  # type: ignore[assignment,union-attr] # could be None
            semantic_types=allowed_semantic_types_passthrough
        )

        column_name: str

        allowed_column_names_passthrough: List[str] = [
            column_name
            for column_name in effective_column_names
            if self.semantic_type_filter.table_column_name_to_inferred_semantic_domain_type_map[  # type: ignore[operator,union-attr] # could be None
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
            column_names=effective_column_names, batch_ids=batch_ids
        )

        if validator is None:
            raise gx_exceptions.ProfilerExecutionError(
                message=f"Error: Failed to obtain Validator {self.__class__.__name__} (Validator is required for cardinality checks)."
            )

        candidate_column_names: List[
            str
        ] = self._column_names_meeting_cardinality_limit(
            validator=validator,
            metrics_for_cardinality_check=metrics_for_cardinality_check,
            runtime_configuration=runtime_configuration,
        )
        candidate_column_names.extend(allowed_column_names_passthrough)

        domains: List[Domain] = build_domains_from_column_names(
            rule_name=rule_name,
            column_names=candidate_column_names,
            domain_type=self.domain_type,
            table_column_name_to_inferred_semantic_domain_type_map=self.semantic_type_filter.table_column_name_to_inferred_semantic_domain_type_map,  # type: ignore[union-attr] # could be None
        )

        return domains

    def _generate_metric_configurations_to_check_cardinality(
        self,
        column_names: List[str],
        batch_ids: Optional[List[str]] = None,
    ) -> Dict[str, List[MetricConfiguration]]:
        """Generate metric configurations used to compute metrics for checking cardinality.

        Args:
            column_names: List of column_names used to create metric configurations.
            batch_ids: List of batch_ids used to create metric configurations.

        Returns:
            Dictionary of the form {
                "my_column_name": List[MetricConfiguration],
            }
        """
        batch_ids = batch_ids or []

        cardinality_limit_mode: Union[
            AbsoluteCardinalityLimit, RelativeCardinalityLimit
        ] = self.cardinality_checker.cardinality_limit_mode  # type: ignore[union-attr] # could be None

        batch_id: str
        metric_configurations: Dict[str, List[MetricConfiguration]] = {
            column_name: [
                MetricConfiguration(
                    metric_name=cardinality_limit_mode.metric_name_defining_limit,
                    metric_domain_kwargs={
                        "column": column_name,
                        "batch_id": batch_id,
                    },
                    metric_value_kwargs=None,
                )
                for batch_id in batch_ids
            ]
            for column_name in column_names
        }

        return metric_configurations

    def _column_names_meeting_cardinality_limit(
        self,
        validator: Validator,
        metrics_for_cardinality_check: Dict[str, List[MetricConfiguration]],
        runtime_configuration: Optional[dict] = None,
    ) -> List[str]:
        """Compute cardinality and return column names meeting cardinality limit.

        Args:
            validator: Validator used to compute column cardinality.
            metrics_for_cardinality_check: metric configurations used to compute cardinality.
            runtime_configuration: Additional run-time settings (see "Validator.DEFAULT_RUNTIME_CONFIGURATION").

        Returns:
            List of column names meeting cardinality.
        """
        column_name: str
        resolved_metrics: Dict[Tuple[str, str, str], MetricValue]
        metric_value: MetricValue

        resolved_metrics_by_column_name: Dict[
            str, Dict[Tuple[str, str, str], MetricValue]
        ] = get_resolved_metrics_by_key(
            validator=validator,
            metric_configurations_by_key=metrics_for_cardinality_check,
            runtime_configuration=runtime_configuration,
        )

        candidate_column_names: List[str] = [
            column_name
            for column_name, resolved_metrics in resolved_metrics_by_column_name.items()
            if all(
                self.cardinality_checker.cardinality_within_limit(  # type: ignore[union-attr] # could be None
                    metric_value=metric_value  # type: ignore[arg-type] # Expecting Union[int, float] (subset of "MetricValue").
                )
                for metric_value in resolved_metrics.values()
            )
        ]

        return candidate_column_names
