from enum import Enum
from typing import Iterable

from ...validator.validation_graph import MetricConfiguration
from .column_domain_builder import ColumnDomainBuilder


class SimpleSemanticTypeColumnDomainBuilder(ColumnDomainBuilder):
    class SemanticDomainTypes(Enum):
        INTEGER = "integer"
        DATETIME = "datetime"

    def _get_domains(
        self, *, validator, batch_ids, include_batch_id, domain_type, **kwargs
    ):
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
        config = kwargs
        type_filter = config.get("type")
        if type_filter is None:
            # None indicates no selection; all types should be returned
            pass
        elif isinstance(type_filter, str):
            type_filter = [self.SemanticDomainTypes[type_filter]]
        elif isinstance(type_filter, Iterable):
            type_filter = [self.SemanticDomainTypes[x] for x in type_filter]
        else:
            raise ValueError("unrecognized ")
        columns = validator.get_metric(MetricConfiguration("table.columns", dict()))
        domains = []
        for column in columns:
            column_type = self._get_column_semantic_type(validator, column)
            if column_type in type_filter:
                domains.append(
                    {
                        "domain_kwargs": {"column": column.name},
                        "domain_type": str(column_type),
                    }
                )
        return domains

    def _get_column_semantic_type(self, validator, column):
        # FIXME: DO CHECKS
        return self.SemanticDomainTypes["INTEGER"]
