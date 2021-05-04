from typing import List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.domain_types import MetricDomainTypes, SemanticDomainTypes
from great_expectations.profiler.domain_builder.column_domain_builder import (
    ColumnDomainBuilder,
)
from great_expectations.profiler.domain_builder.domain import Domain
from great_expectations.profiler.domain_builder.inferred_semantic_domain_type import (
    InferredSemanticDomainType,
)
from great_expectations.profiler.util import (
    infer_semantic_domain_type_from_table_column_type,
    parse_semantic_domain_type_argument,
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
        domain_type: Optional[MetricDomainTypes] = None,
        **kwargs,
    ) -> List[Domain]:
        """
        Find the semantic column type for each column and return all domains matching the specified type or types.
        """
        if validator is None:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"{self.__class__.__name__} requires a reference to an instance of the Validator class."
            )

        config: dict = kwargs
        semantic_types: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ] = config.get("semantic_types")
        if semantic_types is None:
            semantic_types = self._semantic_types
        else:
            semantic_types = parse_semantic_domain_type_argument(
                semantic_types=semantic_types
            )

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
                infer_semantic_domain_type_from_table_column_type(
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
