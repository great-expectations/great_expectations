from typing import Iterable, List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.domain_types import MetricDomainTypes, SemanticDomainTypes
from great_expectations.profiler.domain_builder.column_domain_builder import (
    ColumnDomainBuilder,
)
from great_expectations.profiler.domain_builder.domain import Domain
from great_expectations.profiler.domain_builder.util import (
    translate_table_column_type_to_semantic_domain_type,
)
from great_expectations.validator.validator import MetricConfiguration, Validator


class SimpleSemanticTypeColumnDomainBuilder(ColumnDomainBuilder):
    """
    This DomainBuilder utilizes a "best-effort" semantic interpretation of ("storage") columns of a table.
    """

    def __init__(self, semantic_types: Optional[List[str]] = None):
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
        semantic_types: Union[str, Iterable, List[str]] = config.get("semantic_types")
        semantic_type: str
        if semantic_types is None:
            semantic_types = self._semantic_types
        elif isinstance(semantic_types, str):
            semantic_types = [
                SemanticDomainTypes[semantic_type]
                for semantic_type in [semantic_types]
                if SemanticDomainTypes.has_member_key(key=semantic_type)
            ]
        elif isinstance(semantic_types, (Iterable, List)):
            semantic_types = [
                SemanticDomainTypes[semantic_type]
                for semantic_type in semantic_types
                if SemanticDomainTypes.has_member_key(key=semantic_type)
            ]
        else:
            raise ValueError(
                "Unrecognized semantic_types directive -- must be a list or a string."
            )

        column_names: List[str] = validator.get_metric(
            metric=MetricConfiguration(
                metric_name="table.columns",
                metric_domain_kwargs={},
                metric_value_kwargs=None,
                metric_dependencies=None,
            )
        )
        domains: List[Domain] = []
        column_name: str
        # A semantic type is distinguished from the column storage type;
        # An example of storage type would be "integer".  The semantic type would be "id".
        semantic_column_type: str
        for column_name in column_names:
            semantic_column_type: SemanticDomainTypes = (
                translate_table_column_type_to_semantic_domain_type(
                    validator=validator, column_name=column_name
                )
            )
            if semantic_column_type in semantic_types:
                domains.append(
                    Domain(
                        domain_kwargs={
                            "column": column_name,
                            "batch_id": validator.active_batch_id,
                        },
                        domain_type=semantic_column_type,
                    )
                )

        return domains
