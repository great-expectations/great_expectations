from typing import Any, Dict, List, Optional, Union, cast

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.domain_types import MetricDomainTypes, SemanticDomainTypes
from great_expectations.profile.base import ProfilerTypeMapping
from great_expectations.profiler.domain_builder.column_domain_builder import (
    ColumnDomainBuilder,
)
from great_expectations.profiler.domain_builder.domain import Domain
from great_expectations.profiler.domain_builder.inferred_semantic_domain_type import (
    InferredSemanticDomainType,
)
from great_expectations.validator.validator import MetricConfiguration, Validator


class MyCustomSemanticTypeColumnDomainBuilder(ColumnDomainBuilder):
    """
    This custom DomainBuilder defines and filters for "user_id" semantic type fields
    """

    def __init__(
        self,
        semantic_types: Optional[
            Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
        ] = None,
        column_name_suffixes: Optional[List[str]] = None,
    ):
        if semantic_types is None:
            semantic_types = ["user_id"]
        self._semantic_types = semantic_types

        if column_name_suffixes is None:
            column_name_suffixes = [
                "_id",
            ]
        self._column_name_suffixes = column_name_suffixes

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
            semantic_types = _parse_semantic_domain_type_argument(
                semantic_types=semantic_types
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
        candidate_column_names: List[str] = []

        # First check the column name ends in "_id"
        for column_name in column_names:
            if column_name.endswith(tuple(self._column_name_suffixes)):
                candidate_column_names.append(column_name)

        # Then check if it is of integer type
        # Note this is an example and only contains user_id type
        # # TODO: This does not work on csv data, it should
        # candidate_columns: List[str] = []
        # if "user_id" in semantic_types:
        #     for candidate_column_name in candidate_column_names:
        #         column_types_dict_list: List[Dict[str, Any]] = list(
        #             filter(
        #                 lambda column_type_dict: column_name in column_type_dict,
        #                 validator.get_metric(
        #                     metric=MetricConfiguration(
        #                         metric_name="table.column_types",
        #                         metric_domain_kwargs={},
        #                         metric_value_kwargs={
        #                             "include_nested": True,
        #                         },
        #                         metric_dependencies=None,
        #                     )
        #                 ),
        #             )
        #         )
        #         candidate_column_type: str = cast(str, column_types_dict_list[0][candidate_column_name]).upper()
        #         if candidate_column_type in (
        #                 set([type_name.upper() for type_name in ProfilerTypeMapping.INT_TYPE_NAMES])):
        for candidate_column_name in candidate_column_names:
            domains.append(
                Domain(
                    domain_kwargs={
                        "column": candidate_column_name,
                        "batch_id": validator.active_batch_id,
                    },
                    domain_type=SemanticDomainTypes.NUMERIC,
                )
            )

        return domains


def _parse_semantic_domain_type_argument(
    semantic_types: Optional[
        Union[str, SemanticDomainTypes, List[Union[str, SemanticDomainTypes]]]
    ] = None
) -> List[SemanticDomainTypes]:
    if semantic_types is None:
        return []

    semantic_type: Union[str, SemanticDomainTypes]
    if isinstance(semantic_types, str):
        return [
            SemanticDomainTypes[semantic_type]
            for semantic_type in [semantic_types]
            if SemanticDomainTypes.has_member_key(key=semantic_type)
        ]
    if isinstance(semantic_types, SemanticDomainTypes):
        return [semantic_type for semantic_type in [semantic_types]]
    elif isinstance(semantic_types, List):
        if all([isinstance(semantic_type, str) for semantic_type in semantic_types]):
            return [
                SemanticDomainTypes[semantic_type]
                for semantic_type in semantic_types
                if SemanticDomainTypes.has_member_key(key=semantic_type)
            ]
        elif all(
            [
                isinstance(semantic_type, SemanticDomainTypes)
                for semantic_type in semantic_types
            ]
        ):
            return [semantic_type for semantic_type in semantic_types]
        else:
            raise ValueError(
                "All elements in semantic_types list must be either of str or SemanticDomainTypes type."
            )
    else:
        raise ValueError("Unrecognized semantic_types directive.")
