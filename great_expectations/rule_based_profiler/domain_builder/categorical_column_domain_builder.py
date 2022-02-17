import enum
from dataclasses import dataclass
from typing import List, Optional, Union

from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.domain_builder.domain_builder import (
    build_domains_from_column_names,
)
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validator import Validator


@dataclass
class ProportionalCardinalityLimit:
    name: str
    max_proportion_unique: float


@dataclass
class AbsoluteCardinalityLimit:
    name: str
    max_unique_values: int


class CardinalityCategory(enum.Enum):
    """Used to determine appropriate Expectation configurations based on data.

    Defines relative and absolute number of records (table rows) that
    correspond to each cardinality category.

    """

    VERY_FEW = AbsoluteCardinalityLimit("very_few", 60)
    # TODO AJB 20220216: add implementation
    raise NotImplementedError


class CategoricalColumnDomainBuilder(DomainBuilder):
    """ """

    def __init__(
        self,
        data_context: "DataContext",  # noqa: F821
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
        cardinality_limit: Optional[Union[CardinalityCategory, str]] = None,
    ):
        """Filter columns with unique values no greater than cardinality_limit

        Args:
            data_context: DataContext associated with this profiler
            batch_request: BatchRequest to be optionally used to define batches
                to consider for this domain builder
            cardinality_limit: CardinalityCategory to use when filtering columns

        """

        super().__init__(
            data_context=data_context,
            batch_request=batch_request,
        )

        if cardinality_limit is None:
            cardinality_limit = CardinalityCategory.VERY_FEW.value

        self._cardinality_limit = cardinality_limit

    @property
    def cardinality_limit(self) -> CardinalityCategory:
        """Process cardinality_limit which is passed as a string from config.

        Returns:
            CardinalityCategory default if no cardinality_limit is set,
            the corresponding CardinalityCategory if passed as a string or
            a passthrough if supplied as CardinalityCategory.

        """
        if self._cardinality_limit is None:
            return CardinalityCategory.VERY_FEW.value
        elif isinstance(self._cardinality_limit, str):
            return CardinalityCategory[self._cardinality_limit.upper()].value
        else:
            return self._cardinality_limit.value

    def _get_domains(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        """Return domains matching the selected cardinality_limit.

        Cardinality categories define a maximum number of unique items that
        can be contained in a given domain. If this number is exceeded, the
        domain is not included for the currently executing rule.
        This filter considers unique values across all supplied batches.

        Args:
            variables: Optional variables to substitute when evaluating.

        Returns:
            List of domains that match the desired cardinality.
        """

        batch_id: str = self.get_batch_id(variables=variables)
        table_column_names: List[str] = self.get_validator(
            variables=variables
        ).get_metric(
            metric=MetricConfiguration(
                metric_name="table.columns",
                metric_domain_kwargs={
                    "batch_id": batch_id,
                },
                metric_value_kwargs=None,
                metric_dependencies=None,
            )
        )

        column_names_meeting_cardinality_limit: List[str] = [
            column_name
            for column_name in table_column_names
            if self._column_cardinality_within_limit(
                column=column_name, variables=variables
            )
        ]

        column_domains: List[Domain] = build_domains_from_column_names(
            column_names_meeting_cardinality_limit
        )

        return column_domains

    def _column_cardinality_within_limit(
        self,
        column: str,
        variables: Optional[ParameterContainer] = None,
    ) -> bool:

        validator: Validator = self.get_validator(variables=variables)

        batch_id: str = self.get_batch_id(variables=variables)

        cardinality_limit: CardinalityCategory = self.cardinality_limit

        if isinstance(cardinality_limit, AbsoluteCardinalityLimit):

            column_distinct_values_count: int = validator.get_metric(
                metric=MetricConfiguration(
                    metric_name="column.distinct_values.count",
                    metric_domain_kwargs={
                        "batch_id": batch_id,
                        "column": column,
                    },
                    metric_value_kwargs=None,
                    metric_dependencies=None,
                )
            )
            return column_distinct_values_count <= cardinality_limit.max_unique_values

        elif isinstance(cardinality_limit, ProportionalCardinalityLimit):

            column_unique_proportion = validator.get_metric(
                metric=MetricConfiguration(
                    metric_name="column.unique_proportion",
                    metric_domain_kwargs={
                        "batch_id": batch_id,
                        "column": column,
                    },
                    metric_value_kwargs=None,
                    metric_dependencies=None,
                )
            )
            return column_unique_proportion <= cardinality_limit.max_proportion_unique
