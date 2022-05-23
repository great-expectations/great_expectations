from typing import Iterable, List, Optional, Set, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.helpers.util import (
    build_domains_from_column_names,
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterContainer,
    SemanticDomainTypes,
    SemanticTypeFilter,
)
from great_expectations.validator.metric_configuration import MetricConfiguration


class ColumnDomainBuilder(DomainBuilder):
    exclude_field_names: Set[str] = DomainBuilder.exclude_field_names | {
        "semantic_type_filter",
    }

    def __init__(
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
        data_context: Optional["BaseDataContext"] = None,  # noqa: F821
    ) -> None:
        """
        A semantic type is distinguished from the structured column type;
        An example structured column type would be "integer".  The inferred semantic type would be "id".

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
            data_context: BaseDataContext associated with this DomainBuilder

        Inclusion/Exclusion Logic:
        (include_column_names|table_columns - exclude_column_names) + (include_semantic_types - exclude_semantic_types)
        """
        super().__init__(data_context=data_context)

        self._include_column_names = include_column_names
        self._exclude_column_names = exclude_column_names

        self._include_column_name_suffixes = include_column_name_suffixes
        self._exclude_column_name_suffixes = exclude_column_name_suffixes

        self._semantic_type_filter_module_name = semantic_type_filter_module_name
        self._semantic_type_filter_class_name = semantic_type_filter_class_name

        self._include_semantic_types = include_semantic_types
        self._exclude_semantic_types = exclude_semantic_types

        self._semantic_type_filter = None

    @property
    def domain_type(self) -> MetricDomainTypes:
        return MetricDomainTypes.COLUMN

    """
    All DomainBuilder classes, whose "domain_type" property equals "MetricDomainTypes.COLUMN", must extend present class
    (ColumnDomainBuilder) in order to provide full getter/setter accessor for relevant properties (as overrides).
    """

    @property
    def include_column_names(self) -> Optional[Union[str, Optional[List[str]]]]:
        return self._include_column_names

    @include_column_names.setter
    def include_column_names(
        self, value: Optional[Union[str, Optional[List[str]]]]
    ) -> None:
        self._include_column_names = value

    @property
    def exclude_column_names(self) -> Optional[Union[str, Optional[List[str]]]]:
        return self._exclude_column_names

    @exclude_column_names.setter
    def exclude_column_names(
        self, value: Optional[Union[str, Optional[List[str]]]]
    ) -> None:
        self._exclude_column_names = value

    @property
    def include_column_name_suffixes(
        self,
    ) -> Optional[Union[str, Iterable, List[str]]]:
        return self._include_column_name_suffixes

    @include_column_name_suffixes.setter
    def include_column_name_suffixes(
        self, value: Optional[Union[str, Iterable, List[str]]]
    ) -> None:
        self._include_column_name_suffixes = value

    @property
    def exclude_column_name_suffixes(
        self,
    ) -> Optional[Union[str, Iterable, List[str]]]:
        return self._exclude_column_name_suffixes

    @exclude_column_name_suffixes.setter
    def exclude_column_name_suffixes(
        self, value: Optional[Union[str, Iterable, List[str]]]
    ) -> None:
        self._exclude_column_name_suffixes = value

    @property
    def semantic_type_filter_module_name(self) -> Optional[str]:
        return self._semantic_type_filter_module_name

    @property
    def semantic_type_filter_class_name(self) -> Optional[str]:
        return self._semantic_type_filter_class_name

    @property
    def include_semantic_types(
        self,
    ) -> Optional[
        Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
    ]:
        return self._include_semantic_types

    @include_semantic_types.setter
    def include_semantic_types(
        self,
        value: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ],
    ) -> None:
        self._include_semantic_types = value

    @property
    def exclude_semantic_types(
        self,
    ) -> Optional[
        Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
    ]:
        return self._exclude_semantic_types

    @exclude_semantic_types.setter
    def exclude_semantic_types(
        self,
        value: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ],
    ) -> None:
        self._exclude_semantic_types = value

    @property
    def semantic_type_filter(self) -> Optional[SemanticTypeFilter]:
        return self._semantic_type_filter

    def get_effective_column_names(
        self,
        batch_ids: Optional[List[str]] = None,
        validator: Optional["Validator"] = None,  # noqa: F821
        variables: Optional[ParameterContainer] = None,
    ) -> List[str]:
        column_name: str

        # Obtain include_column_names from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        include_column_names: Optional[List[str]] = self.include_column_names
        if include_column_names is None:
            include_column_names = []
        elif isinstance(include_column_names, str):
            include_column_names = [include_column_names]
        else:
            if not isinstance(include_column_names, list):
                raise ValueError(
                    "Unrecognized include_column_names directive -- must be list or string."
                )

        include_column_names = [
            get_parameter_value_and_validate_return_type(
                domain=None,
                parameter_reference=column_name,
                expected_return_type=None,
                variables=variables,
                parameters=None,
            )
            for column_name in include_column_names
        ]

        # Obtain exclude_column_names from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        exclude_column_names: Optional[List[str]] = self.exclude_column_names
        if exclude_column_names is None:
            exclude_column_names = []
        elif isinstance(exclude_column_names, str):
            exclude_column_names = [exclude_column_names]
        else:
            if not isinstance(exclude_column_names, list):
                raise ValueError(
                    "Unrecognized exclude_column_names directive -- must be list or string."
                )

        exclude_column_names = [
            get_parameter_value_and_validate_return_type(
                domain=None,
                parameter_reference=column_name,
                expected_return_type=None,
                variables=variables,
                parameters=None,
            )
            for column_name in exclude_column_names
        ]

        if batch_ids is None:
            batch_ids: List[str] = self.get_batch_ids(variables=variables)

        if validator is None:
            validator = self.get_validator(variables=variables)

        table_columns: List[str] = validator.get_metric(
            metric=MetricConfiguration(
                metric_name="table.columns",
                metric_domain_kwargs={
                    "batch_id": batch_ids[-1],  # active_batch_id
                },
                metric_value_kwargs=None,
                metric_dependencies=None,
            )
        )

        effective_column_names: List[str] = include_column_names or table_columns

        if exclude_column_names is None:
            exclude_column_names = []

        effective_column_names = [
            column_name
            for column_name in effective_column_names
            if column_name not in exclude_column_names
        ]

        for column_name in effective_column_names:
            if column_name not in table_columns:
                raise ge_exceptions.ProfilerExecutionError(
                    message=f'Error: The column "{column_name}" in BatchData does not exist.'
                )

        column_name_suffix: str

        # Obtain include_column_name_suffixes from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        include_column_name_suffixes: Optional[
            Union[str, Iterable, List[str]]
        ] = self.include_column_name_suffixes
        if include_column_name_suffixes is None:
            include_column_name_suffixes = []
        elif isinstance(include_column_name_suffixes, str):
            include_column_name_suffixes = [include_column_name_suffixes]
        else:
            if not isinstance(include_column_name_suffixes, (Iterable, list)):
                raise ValueError(
                    "Unrecognized include_column_name_suffixes directive -- must be list or string."
                )

        include_column_name_suffixes = [
            get_parameter_value_and_validate_return_type(
                domain=None,
                parameter_reference=column_name_suffix,
                expected_return_type=None,
                variables=variables,
                parameters=None,
            )
            for column_name_suffix in include_column_name_suffixes
        ]

        # Obtain exclude_column_name_suffixes from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        exclude_column_name_suffixes: Optional[
            Union[str, Iterable, List[str]]
        ] = self.exclude_column_name_suffixes
        if exclude_column_name_suffixes is None:
            exclude_column_name_suffixes = []
        elif isinstance(exclude_column_name_suffixes, str):
            exclude_column_name_suffixes = [exclude_column_name_suffixes]
        else:
            if not isinstance(include_column_name_suffixes, (Iterable, list)):
                raise ValueError(
                    "Unrecognized exclude_column_name_suffixes directive -- must be list or string."
                )

        exclude_column_name_suffixes = [
            get_parameter_value_and_validate_return_type(
                domain=None,
                parameter_reference=column_name_suffix,
                expected_return_type=None,
                variables=variables,
                parameters=None,
            )
            for column_name_suffix in exclude_column_name_suffixes
        ]

        if include_column_name_suffixes:
            effective_column_names = list(
                filter(
                    lambda candidate_column_name: candidate_column_name.endswith(
                        tuple(include_column_name_suffixes)
                    ),
                    effective_column_names,
                )
            )

        if exclude_column_name_suffixes:
            effective_column_names = list(
                filter(
                    lambda candidate_column_name: not candidate_column_name.endswith(
                        tuple(exclude_column_name_suffixes)
                    ),
                    effective_column_names,
                )
            )

        # Obtain semantic_type_filter_module_name from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        semantic_type_filter_module_name: Optional[
            str
        ] = get_parameter_value_and_validate_return_type(
            domain=None,
            parameter_reference=self.semantic_type_filter_module_name,
            expected_return_type=None,
            variables=variables,
            parameters=None,
        )
        if semantic_type_filter_module_name is None:
            semantic_type_filter_module_name = "great_expectations.rule_based_profiler.helpers.simple_semantic_type_filter"

        # Obtain semantic_type_filter_class_name from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        semantic_type_filter_class_name: Optional[
            str
        ] = get_parameter_value_and_validate_return_type(
            domain=None,
            parameter_reference=self.semantic_type_filter_class_name,
            expected_return_type=None,
            variables=variables,
            parameters=None,
        )
        if semantic_type_filter_class_name is None:
            semantic_type_filter_class_name = "SimpleSemanticTypeFilter"

        semantic_type_filter: SemanticTypeFilter = instantiate_class_from_config(
            config={
                "module_name": semantic_type_filter_module_name,
                "class_name": semantic_type_filter_class_name,
            },
            runtime_environment={
                "batch_ids": batch_ids,
                "validator": validator,
                "column_names": effective_column_names,
            },
            config_defaults={},
        )
        self._semantic_type_filter = semantic_type_filter

        semantic_type: str

        # Obtain include_semantic_types from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        include_semantic_types: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ] = self.include_semantic_types
        if include_semantic_types is None:
            include_semantic_types = []
        elif isinstance(include_semantic_types, str):
            include_semantic_types = [include_semantic_types]
        else:
            if not isinstance(include_semantic_types, (SemanticDomainTypes, list)):
                raise ValueError(
                    'Unrecognized include_semantic_types directive -- must be list or "SemanticDomainTypes" or string.'
                )

        include_semantic_types = [
            get_parameter_value_and_validate_return_type(
                domain=None,
                parameter_reference=semantic_type,
                expected_return_type=None,
                variables=variables,
                parameters=None,
            )
            for semantic_type in include_semantic_types
        ]

        include_semantic_types = (
            self.semantic_type_filter.parse_semantic_domain_type_argument(
                semantic_types=include_semantic_types
            )
        )

        # Obtain exclude_semantic_types from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        exclude_semantic_types: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ] = self.exclude_semantic_types
        if exclude_semantic_types is None:
            exclude_semantic_types = []
        elif isinstance(exclude_semantic_types, str):
            exclude_semantic_types = [exclude_semantic_types]
        else:
            if not isinstance(exclude_semantic_types, (SemanticDomainTypes, list)):
                raise ValueError(
                    'Unrecognized exclude_semantic_types directive -- must be list or "SemanticDomainTypes" or string.'
                )

        exclude_semantic_types = [
            get_parameter_value_and_validate_return_type(
                domain=None,
                parameter_reference=semantic_type,
                expected_return_type=None,
                variables=variables,
                parameters=None,
            )
            for semantic_type in exclude_semantic_types
        ]

        exclude_semantic_types = (
            self.semantic_type_filter.parse_semantic_domain_type_argument(
                semantic_types=exclude_semantic_types
            )
        )

        if include_semantic_types:
            effective_column_names = list(
                filter(
                    lambda candidate_column_name: self.semantic_type_filter.table_column_name_to_inferred_semantic_domain_type_map[
                        candidate_column_name
                    ]
                    in include_semantic_types,
                    effective_column_names,
                )
            )

        if exclude_semantic_types:
            effective_column_names = list(
                filter(
                    lambda candidate_column_name: self.semantic_type_filter.table_column_name_to_inferred_semantic_domain_type_map[
                        candidate_column_name
                    ]
                    not in exclude_semantic_types,
                    effective_column_names,
                )
            )

        return effective_column_names

    def _get_domains(
        self,
        rule_name: str,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        """
        Obtains and returns domains for all columns of a table (or for configured columns, if they exist in the table).
        """
        batch_ids: List[str] = self.get_batch_ids(variables=variables)

        validator: "Validator" = self.get_validator(variables=variables)  # noqa: F821

        effective_column_names: List[str] = self.get_effective_column_names(
            batch_ids=batch_ids,
            validator=validator,
            variables=variables,
        )

        column_name: str
        domains: List[Domain] = build_domains_from_column_names(
            rule_name=rule_name,
            column_names=effective_column_names,
            domain_type=self.domain_type,
            table_column_name_to_inferred_semantic_domain_type_map=self.semantic_type_filter.table_column_name_to_inferred_semantic_domain_type_map,
        )

        return domains
