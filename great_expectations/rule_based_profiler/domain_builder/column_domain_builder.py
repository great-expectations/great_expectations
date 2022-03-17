import copy
from typing import Any, Dict, List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.helpers.util import (
    build_simple_domains_from_column_names,
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.types import (
    Domain,
    ParameterContainer,
    SemanticDomainTypes,
)
from great_expectations.rule_based_profiler.types.semantic_type_filter import (
    SemanticTypeFilter,
)
from great_expectations.validator.metric_configuration import MetricConfiguration


class ColumnDomainBuilder(DomainBuilder):
    def __init__(
        self,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
        semantic_type_filter_module_name: Optional[str] = None,
        semantic_type_filter_class_name: Optional[str] = None,
        include_semantic_types: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ] = None,
        exclude_semantic_types: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ] = None,
        include_column_names: Optional[Union[str, Optional[List[str]]]] = None,
        exclude_column_names: Optional[Union[str, Optional[List[str]]]] = None,
    ):
        """
        A semantic type is distinguished from the structured column type;
        An example structured column type would be "integer".  The inferred semantic type would be "id".

        Args:
            batch_list: explicitly specified Batch objects for use in DomainBuilder
            batch_request: specified in DomainBuilder configuration to get Batch objects for domain computation.
            data_context: DataContext
            semantic_type_filter_module_name: module_name containing class that implements SemanticTypeFilter interfaces
            semantic_type_filter_class_name: class_name of class that implements SemanticTypeFilter interfaces
            include_semantic_types: single/multiple type specifications using SemanticDomainTypes (or str equivalents)
            to be included
            exclude_semantic_types: single/multiple type specifications using SemanticDomainTypes (or str equivalents)
            to be excluded
            include_column_names: Explicitly specified desired columns (if None, it is computed based on active Batch).
            exclude_column_names: If provided, these columns are pre-filtered and excluded from consideration.

        Inclusion/Exclusion Logic:
        (include_column_names|table_columns - exclude_column_names) + (include_semantic_types - exclude_semantic_types)
        """
        super().__init__(
            batch_list=batch_list,
            batch_request=batch_request,
            data_context=data_context,
        )

        self._include_semantic_types = include_semantic_types
        self._exclude_semantic_types = exclude_semantic_types

        self._include_column_names = include_column_names
        self._exclude_column_names = exclude_column_names

        if semantic_type_filter_module_name is None:
            semantic_type_filter_module_name = "great_expectations.rule_based_profiler.helpers.simple_semantic_type_filter"

        self._semantic_type_filter_module_name = semantic_type_filter_module_name

        if semantic_type_filter_class_name is None:
            semantic_type_filter_class_name = "SimpleSemanticTypeFilter"

        self._semantic_type_filter_class_name = semantic_type_filter_class_name

    @property
    def domain_type(self) -> Union[str, MetricDomainTypes]:
        return MetricDomainTypes.COLUMN

    """
    All DomainBuilder classes, whose "domain_type" property equals "MetricDomainTypes.COLUMN", must extend present class
    (ColumnDomainBuilder) in order to provide full getter/setter accessor for relevant properties (as overrides).
    """

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
    ):
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
    ):
        self._exclude_semantic_types = value

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
    def semantic_type_filter_module_name(self) -> str:
        return self._semantic_type_filter_module_name

    @property
    def semantic_type_filter_class_name(self) -> str:
        return self._semantic_type_filter_class_name

    def get_effective_column_names(
        self,
        batch_ids: Optional[List[str]] = None,
        validator: Optional["Validator"] = None,  # noqa: F821
        variables: Optional[ParameterContainer] = None,
    ) -> List[str]:
        # Obtain include_column_names from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        include_column_names: Optional[
            List[str]
        ] = get_parameter_value_and_validate_return_type(
            domain=None,
            parameter_reference=self.include_column_names,
            expected_return_type=None,
            variables=variables,
            parameters=None,
        )

        # Obtain exclude_column_names from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        exclude_column_names: Optional[
            List[str]
        ] = get_parameter_value_and_validate_return_type(
            domain=None,
            parameter_reference=self.exclude_column_names,
            expected_return_type=None,
            variables=variables,
            parameters=None,
        )

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

        column_name: str
        effective_column_names = [
            column_name
            for column_name in effective_column_names
            if column_name not in exclude_column_names
        ]

        if set(effective_column_names) == set(table_columns):
            return effective_column_names

        column_name: str
        for column_name in effective_column_names:
            if column_name not in table_columns:
                raise ge_exceptions.ProfilerExecutionError(
                    message=f'Error: The column "{column_name}" in BatchData does not exist.'
                )

        return effective_column_names

    def _get_domains(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        """
        Obtains and returns domains for all columns of a table (or for configured columns, if they exist in the table).
        """
        batch_ids: List[str] = self.get_batch_ids(variables=variables)

        validator: "Validator" = self.get_validator(variables=variables)  # noqa: F821

        table_column_names: List[str] = self.get_effective_column_names(
            batch_ids=batch_ids,
            validator=validator,
            variables=variables,
        )

        # Obtain semantic_type_filter_module_name from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        semantic_type_filter_module_name: str = (
            get_parameter_value_and_validate_return_type(
                domain=None,
                parameter_reference=self.semantic_type_filter_module_name,
                expected_return_type=str,
                variables=variables,
                parameters=None,
            )
        )

        # Obtain semantic_type_filter_class_name from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        semantic_type_filter_class_name: str = (
            get_parameter_value_and_validate_return_type(
                domain=None,
                parameter_reference=self.semantic_type_filter_class_name,
                expected_return_type=str,
                variables=variables,
                parameters=None,
            )
        )

        semantic_type_filter: SemanticTypeFilter = instantiate_class_from_config(
            config={
                "module_name": semantic_type_filter_module_name,
                "class_name": semantic_type_filter_class_name,
            },
            runtime_environment={},
            config_defaults={},
        )

        # Obtain include_semantic_types from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        include_semantic_types: Union[
            str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]
        ] = get_parameter_value_and_validate_return_type(
            domain=None,
            parameter_reference=self.include_semantic_types,
            expected_return_type=None,
            variables=variables,
            parameters=None,
        )
        include_semantic_types = (
            semantic_type_filter.parse_semantic_domain_type_argument(
                semantic_types=include_semantic_types
            )
        )

        # Obtain exclude_semantic_types from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        exclude_semantic_types: Union[
            str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]
        ] = get_parameter_value_and_validate_return_type(
            domain=None,
            parameter_reference=self.exclude_semantic_types,
            expected_return_type=None,
            variables=variables,
            parameters=None,
        )
        exclude_semantic_types = (
            semantic_type_filter.parse_semantic_domain_type_argument(
                semantic_types=exclude_semantic_types
            )
        )

        if not (self.include_semantic_types or self.exclude_semantic_types):
            return build_simple_domains_from_column_names(
                column_names=table_column_names,
                domain_type=self.domain_type,
            )

        column_types_dict_list: List[Dict[str, Any]] = validator.get_metric(
            metric=MetricConfiguration(
                metric_name="table.column_types",
                metric_domain_kwargs={
                    "batch_id": batch_ids[-1],  # active_batch_id
                },
                metric_value_kwargs={
                    "include_nested": True,
                },
                metric_dependencies=None,
            )
        )

        column_name: str

        table_column_name_to_inferred_semantic_domain_type_mapping: Dict[
            str, SemanticDomainTypes
        ] = {
            column_name: semantic_type_filter.infer_semantic_domain_type_from_table_column_type(
                column_types_dict_list=column_types_dict_list,
                column_name=column_name,
            ).semantic_domain_type
            for column_name in table_column_names
        }

        candidate_column_names: List[str] = copy.deepcopy(table_column_names)

        if include_semantic_types:
            candidate_column_names = list(
                filter(
                    lambda candidate_column_name: table_column_name_to_inferred_semantic_domain_type_mapping[
                        candidate_column_name
                    ]
                    in include_semantic_types,
                    candidate_column_names,
                )
            )

        if exclude_semantic_types:
            candidate_column_names = list(
                filter(
                    lambda candidate_column_name: table_column_name_to_inferred_semantic_domain_type_mapping[
                        candidate_column_name
                    ]
                    not in exclude_semantic_types,
                    candidate_column_names,
                )
            )

        domains: List[Domain] = [
            Domain(
                domain_type=self.domain_type,
                domain_kwargs={
                    "column": column_name,
                },
                details={
                    "inferred_semantic_domain_type": table_column_name_to_inferred_semantic_domain_type_mapping[
                        column_name
                    ],
                },
            )
            for column_name in candidate_column_names
        ]

        return domains
