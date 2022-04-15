from typing import List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import ColumnDomainBuilder
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer


class MultiColumnDomainBuilder(ColumnDomainBuilder):
    """
    This DomainBuilder uses relative tolerance of specified map metric to identify domains.
    """

    def __init__(
        self,
        include_column_names: Optional[Union[str, Optional[List[str]]]] = None,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[
            Union[str, BatchRequest, RuntimeBatchRequest, dict]
        ] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
    ):
        """
        Args:
            include_column_names: Explicitly specified desired columns.
            batch_list: explicitly specified Batch objects for use in DomainBuilder
            batch_request: BatchRequest to be optionally used to define batches to consider for this domain builder.
            data_context: DataContext associated with this profiler.
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
            batch_list=batch_list,
            batch_request=batch_request,
            data_context=data_context,
        )

    @property
    def domain_type(self) -> MetricDomainTypes:
        return MetricDomainTypes.MULTICOLUMN

    def _get_domains(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        """Return domains matching the specified tolerance limits.

        Args:
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

        domains: List[Domain] = [
            Domain(
                domain_type=self.domain_type,
                domain_kwargs={
                    "column_list": effective_column_names,
                },
            ),
        ]

        return domains
