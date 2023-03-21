from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    ClassVar,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)


from dataprofiler import Profiler

from great_expectations.core.domain import Domain, SemanticDomainTypes
from great_expectations.rule_based_profiler.domain_builder import (
    ColumnDomainBuilder,
)
from great_expectations.rule_based_profiler.helpers.util import (
    build_domains_from_column_names,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,  # noqa: TCH001
)

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.validator.validator import Validator


class DataProfilerColumnDomainBuilder(ColumnDomainBuilder):

    def __init__(
        self,
        include_column_names: Optional[Union[str, Optional[List[str]]]] = None,
        exclude_column_names: Optional[Union[str, Optional[List[str]]]] = None,
        include_column_name_suffixes: Optional[Union[str,
                                                     Iterable, List[str]]] = None,
        exclude_column_name_suffixes: Optional[Union[str,
                                                     Iterable, List[str]]] = None,
        semantic_type_filter_module_name: Optional[str] = None,
        semantic_type_filter_class_name: Optional[str] = None,
        include_semantic_types: Optional[
            Union[str, SemanticDomainTypes,
                  List[Union[str, SemanticDomainTypes]]]
        ] = None,
        exclude_semantic_types: Optional[
            Union[str, SemanticDomainTypes,
                  List[Union[str, SemanticDomainTypes]]]
        ] = None,
        data_context: Optional[AbstractDataContext] = None,
    ) -> None:
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

    def _get_domains(
        self,
        rule_name: str,
        variables: Optional[ParameterContainer] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> List[Domain]:
        """
        Obtains and returns domains for all columns of a table that match a rule.
        Args:
            rule_name: name of Rule object, for which "Domain" objects are obtained.
            variables: Optional variables to substitute when evaluating.
            runtime_configuration: Optional[dict] = None,
        Returns:
            List of domains that match the desired columns and filtering criteria.
        """
        batch_ids: List[str] = self.get_batch_ids(
            variables=variables)  # type: ignore[assignment] # could be None

        # type: ignore[assignment] # could be None
        validator: Validator = self.get_validator(variables=variables)

        effective_column_names: List[str] = self.get_effective_column_names(
            batch_ids=batch_ids,
            validator=validator,
            variables=variables,
            rule_name=rule_name
        )

        column_name: str
        domains: List[Domain] = build_domains_from_column_names(
            rule_name=rule_name,
            column_names=effective_column_names,
            domain_type=self.domain_type,
            # type: ignore[union-attr] # could be None
            table_column_name_to_inferred_semantic_domain_type_map=None,
            # self.semantic_type_filter.table_column_name_to_inferred_semantic_domain_type_map,
        )

        return domains

    def get_effective_column_names(
        self,
        batch_ids: Optional[List[str]] = None,
        validator: Optional[Validator] = None,
        variables: Optional[ParameterContainer] = None,
        rule_name: Optional[str] = None
    ) -> List[str]:
        """
        This method filters by a rule's criteria to obtain columns to be included as part of returned "Domain" objects.
        """

        effective_column_names = list()

        rule_name_to_data_types = {"numeric_rule": {"int", "float"}, "timestamp_rule": {
            "datetime"}, "text_rule": {"string", "text"}, "categorical_rule": {"string"}}

        if not variables:
            raise TypeError("Variables not defined")

        if rule_name not in rule_name_to_data_types:
            # change exception to more specific one
            raise Exception(f"'{rule_name}' is not a valid Rule name")

        profile_path: str = variables["parameter_nodes"]["variables"]["variables"]["profile_path"]

        profile = Profiler.load(profile_path)

        report = profile.report(report_options={"output_format": "compact"})

        data_types_from_rule = rule_name_to_data_types[rule_name.lower()]

        if report["data_stats"] == None:
            return effective_column_names

        for col in report["data_stats"]:
            if col["data_type"] == None:
                continue
            if col["data_type"].lower() in data_types_from_rule:
                effective_column_names.append(col["column_name"])

        return effective_column_names