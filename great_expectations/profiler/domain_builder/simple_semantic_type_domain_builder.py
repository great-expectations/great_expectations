from enum import Enum
from typing import Iterable, List, Optional

from great_expectations.validator.validator import Validator

from ...validator.validation_graph import MetricConfiguration
from .column_domain_builder import ColumnDomainBuilder


class SimpleSemanticTypeColumnDomainBuilder(ColumnDomainBuilder):
    def __init__(self, type_filters: Optional[List[str]] = None):
        if type_filters is None:
            type_filters = []
        self._type_filters = type_filters

    class SemanticDomainTypes(Enum):
        INTEGER = "integer"
        DATETIME = "datetime"

    def _get_domains(
        self,
        *,
        validator: Optional[Validator] = None,
        batch_ids: Optional[List[str]] = None,
        include_batch_id: Optional[bool] = False,
        # TODO: <Alex>ALEX -- The signature of this method is inconsistent with that in the base class.</Alex>
        # domain_type: Optional[MetricDomainTypes] = None,
        type_filters: Optional[List[str]] = None,
        **kwargs
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
        # TODO: <Alex>ALEX -- How does this work?  What information is provided in **kwargs?  Can this be made explicit?  Where is "_type_filters" defined?</Alex>
        config = kwargs
        # TODO: AJB 20210416 If the type keyword for the DomainBuilder can contain multiple semantic types, should it be renamed types and take a list instead? Not that we can’t guess from what a user adds but something to make it clear that multiple semantic types can be used to construct a domain?
        # TODO: <Alex>ALEX -- In general, to avoid confusion, we should avoid the use of "type" because it is a function in Python.</Alex>
        type_filters = config.get("type_filters")
        if type_filters is None:
            # TODO: AJB 20210416 Add a test for the below comment - None = return all types
            # None indicates no selection; all types should be returned
            type_filters = self._type_filters
        elif isinstance(type_filters, str):
            type_filters = [self.SemanticDomainTypes[type_filters]]
        elif isinstance(type_filters, Iterable):
            type_filters = [self.SemanticDomainTypes[x] for x in type_filters]
        else:
            # TODO: <Alex>ALEX -- We should make this error message more informative.</Alex>
            raise ValueError("unrecognized")
        columns = validator.get_metric(MetricConfiguration("table.columns", dict()))
        domains = []
        for column in columns:
            column_type = self._get_column_semantic_type(
                validator=validator, column=column
            )
            if column_type in type_filters:
                domains.append(
                    {
                        # TODO: AJB 20210419 why is column just the column name string - will this be different based on execution engine versions?
                        # "domain_kwargs": {"column": column.name},
                        "domain_kwargs": {"column": column.name},
                        "domain_type": column_type,
                    }
                )
        return domains

    # TODO: <Alex>ALEX -- This method seems to always return the same value ("integer")...</Alex>
    def _get_column_semantic_type(self, validator, column):
        # FIXME: DO CHECKS
        return self.SemanticDomainTypes["INTEGER"].value
