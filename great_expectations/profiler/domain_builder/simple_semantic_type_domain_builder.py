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
        # TODO: AJB 20210416 If the type keyword for the DomainBuilder can contain multiple semantic types, should it be renamed types and take a list instead? Not that we can’t guess from what a user adds but something to make it clear that multiple semantic types can be used to construct a domain?
        type_filters = config.get("type")
        if type_filters is None:
            # None indicates no selection; all types should be returned
            type_filters = []
        elif isinstance(type_filters, str):
            type_filters = [self.SemanticDomainTypes[type_filters]]
        elif isinstance(type_filters, Iterable):
            type_filters = [self.SemanticDomainTypes[x] for x in type_filters]
        else:
            raise ValueError("unrecognized ")
        columns = validator.get_metric(MetricConfiguration("table.columns", dict()))
        domains = []
        for column in columns:
            column_type = self._get_column_semantic_type(validator, column)
            if column_type in type_filters:
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
