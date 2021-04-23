from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Union

from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.profiler.domain_builder.column_domain_builder import (
    ColumnDomainBuilder,
)
from great_expectations.validator.validator import MetricConfiguration, Validator


class SimpleSemanticTypeColumnDomainBuilder(ColumnDomainBuilder):
    def __init__(self, semantic_type_filters: Optional[List[str]] = None):
        if semantic_type_filters is None:
            semantic_type_filters = []
        self._semantic_type_filters = semantic_type_filters

    class SemanticDomainTypes(Enum):
        INTEGER = "integer"
        DATETIME = "datetime"

    def _get_domains(
        self,
        *,
        validator: Optional[Validator] = None,
        batch_ids: Optional[List[str]] = None,
        include_batch_id: Optional[bool] = False,
        domain_type: Optional[MetricDomainTypes] = None,
        **kwargs
    ) -> List[Dict[str, Union[str, MetricDomainTypes, Dict[str, Any]]]]:
        """Find the semantic column type for each column and return all domains matching the specified type or types.

        Returns a list:
        [
            {
                domain,
                domain_type
            },
            ...
        ]
        """
        config: dict = kwargs
        semantic_type_filters: Union[str, Iterable, List[str]] = config.get(
            "semantic_type_filters"
        )
        if semantic_type_filters is None:
            semantic_type_filters = self._semantic_type_filters
        elif isinstance(semantic_type_filters, str):
            semantic_type_filters = [self.SemanticDomainTypes[semantic_type_filters]]
        elif isinstance(semantic_type_filters, Iterable):
            semantic_type_filters = [
                self.SemanticDomainTypes[x] for x in semantic_type_filters
            ]
        else:
            # TODO: <Alex>ALEX -- We should make semantic_type_filters into a more robust data structure (i.e., a more sophisticated filtering object), or change the variable name to indicate that this is just the list of types (or a single string).</Alex>
            raise ValueError(
                "Unrecognized type of semantic domain filters directive -- must be a list or a string."
            )

        domains: List[Dict[str, Union[str, MetricDomainTypes, Dict[str, Any]]]] = []
        columns: List[str] = validator.get_metric(
            metric=MetricConfiguration(
                metric_name="table.columns",
                metric_domain_kwargs={},
                metric_value_kwargs=None,
                metric_dependencies=None,
            )
        )
        # TODO: <Alex>ALEX -- How can/should we use "batch_id" and "include_batch_id"?</Alex>
        column: str
        # A semantic type is distinguished from the column storage type;
        # An example of storage type would be "integer".  The semantic type would be "id".
        semantic_column_type: str
        for column in columns:
            semantic_column_type: str = self._get_column_semantic_type_name(
                validator=validator, column=column
            )
            if semantic_column_type in semantic_type_filters:
                domains.append(
                    {
                        "domain_kwargs": {"column": column},
                        "domain_type": semantic_column_type,
                    }
                )
        return domains

    # TODO: <Alex>ALEX -- This method seems to always return the same value ("integer")...</Alex>
    def _get_column_semantic_type_name(self, validator, column) -> str:
        # FIXME: DO CHECKS
        return self.SemanticDomainTypes["INTEGER"].value
