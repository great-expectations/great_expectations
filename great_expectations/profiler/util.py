from typing import Any, Dict, List, Optional, Union, cast

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.core.batch import BatchDefinition, BatchRequest
from great_expectations.core.domain_types import SemanticDomainTypes
from great_expectations.profile.base import ProfilerTypeMapping
from great_expectations.profiler.domain_builder.domain import Domain
from great_expectations.profiler.domain_builder.inferred_semantic_domain_type import (
    InferredSemanticDomainType,
)
from great_expectations.profiler.parameter_builder.parameter_container import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    DOMAIN_KWARGS_PARAMETER_NAME,
    VARIABLES_KEY,
    ParameterContainer,
    ParameterNode,
    validate_fully_qualified_parameter_name,
)
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator


def infer_semantic_domain_type_from_table_column_type(
    validator: Validator, column_name: str
) -> InferredSemanticDomainType:
    # Note: As of Python 3.8, specifying argument type in Lambda functions is not supported by Lambda syntax.
    column_types_dict_list: List[Dict[str, Any]] = list(
        filter(
            lambda column_type_dict: column_name in column_type_dict,
            validator.get_metric(
                metric=MetricConfiguration(
                    metric_name="table.column_types",
                    metric_domain_kwargs={},
                    metric_value_kwargs={
                        "include_nested": True,
                    },
                    metric_dependencies=None,
                )
            ),
        )
    )
    if len(column_types_dict_list) != 1:
        raise ge_exceptions.ProfilerExecutionError(
            message=f"""Error: {len(column_types_dict_list)} columns were found while obtaining semantic type \
information.  Please ensure that the specified column name refers to exactly one column.
"""
        )

    column_type: str = cast(str, column_types_dict_list[0][column_name]).upper()

    semantic_column_type: SemanticDomainTypes
    if column_type in (
        set([type_name.upper() for type_name in ProfilerTypeMapping.INT_TYPE_NAMES])
        | set([type_name.upper() for type_name in ProfilerTypeMapping.FLOAT_TYPE_NAMES])
    ):
        semantic_column_type = SemanticDomainTypes.NUMERIC
    elif column_type in set(
        [type_name.upper() for type_name in ProfilerTypeMapping.STRING_TYPE_NAMES]
    ):
        semantic_column_type = SemanticDomainTypes.TEXT
    elif column_type in set(
        [type_name.upper() for type_name in ProfilerTypeMapping.BOOLEAN_TYPE_NAMES]
    ):
        semantic_column_type = SemanticDomainTypes.LOGIC
    elif column_type in set(
        [type_name.upper() for type_name in ProfilerTypeMapping.DATETIME_TYPE_NAMES]
    ):
        semantic_column_type = SemanticDomainTypes.DATETIME
    elif column_type in set(
        [type_name.upper() for type_name in ProfilerTypeMapping.BINARY_TYPE_NAMES]
    ):
        semantic_column_type = SemanticDomainTypes.BINARY
    elif column_type in set(
        [type_name.upper() for type_name in ProfilerTypeMapping.CURRENCY_TYPE_NAMES]
    ):
        semantic_column_type = SemanticDomainTypes.CURRENCY
    elif column_type in set(
        [type_name.upper() for type_name in ProfilerTypeMapping.IDENTITY_TYPE_NAMES]
    ):
        semantic_column_type = SemanticDomainTypes.IDENTITY
    elif column_type in (
        set(
            [
                type_name.upper()
                for type_name in ProfilerTypeMapping.MISCELLANEOUS_TYPE_NAMES
            ]
        )
        | set(
            [type_name.upper() for type_name in ProfilerTypeMapping.RECORD_TYPE_NAMES]
        )
    ):
        semantic_column_type = SemanticDomainTypes.MISCELLANEOUS
    else:
        semantic_column_type = SemanticDomainTypes.UNKNOWN

    inferred_semantic_column_type: InferredSemanticDomainType = (
        InferredSemanticDomainType(
            semantic_domain_type=semantic_column_type,
            details={
                "algorithm_type": "deterministic",
                "mechanism": "lookup_table",
                "source": "great_expectations.profile.base.ProfilerTypeMapping",
            },
        )
    )

    return inferred_semantic_column_type


def parse_semantic_domain_type_argument(
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


def get_parameter_value(
    fully_qualified_parameter_name: str,
    domain: Domain,
    rule_variables: Optional[ParameterContainer] = None,
    rule_domain_parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional[Any]:
    """
    Get the parameter value from the current rule state using the fully-qualified parameter name.
    A fully-qualified parameter name must be a dot-delimited string, or the name of a parameter (without the dots).
    Args
        :param fully_qualified_parameter_name: str -- A dot-separated string key starting with $ for fetching parameters
        :param domain: Domain -- current Domain of interest
        :param rule_variables
        :param rule_domain_parameters
    :return: value
    """
    validate_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name
    )

    if fully_qualified_parameter_name == DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME:
        # Using "__getitem__" (bracket) notation instead of "__getattr__" (dot) notation in order to insure the
        # compatibility of field names (e.g., "domain_kwargs") with user-facing syntax (as governed by the value of
        # the DOMAIN_KWARGS_PARAMETER_NAME constant, which may change, requiring the same change to the field name).
        return domain[DOMAIN_KWARGS_PARAMETER_NAME]

    fully_qualified_parameter_name_references_variable: bool = False
    if fully_qualified_parameter_name.startswith(VARIABLES_KEY):
        fully_qualified_parameter_name = fully_qualified_parameter_name[
            len(VARIABLES_KEY) :
        ]
        fully_qualified_parameter_name_references_variable = True
    else:
        fully_qualified_parameter_name = fully_qualified_parameter_name[1:]

    fully_qualified_parameter_as_list: List[str] = fully_qualified_parameter_name.split(
        "."
    )

    if len(fully_qualified_parameter_as_list) == 0:
        return None

    parameter_container: ParameterContainer
    if fully_qualified_parameter_name_references_variable:
        parameter_container = rule_variables
    else:
        parameter_container = rule_domain_parameters[domain.id]

    parameter_node: Optional[ParameterNode] = parameter_container.get_parameter_node(
        parameter_name_root=fully_qualified_parameter_as_list[0]
    )

    parameter_name_part: Optional[str] = None
    try:
        for parameter_name_part in fully_qualified_parameter_as_list:
            if (
                parameter_node.attributes
                and parameter_name_part in parameter_node.attributes
            ):
                return parameter_node.attributes[parameter_name_part]

            parameter_node = parameter_node.descendants[parameter_name_part]
    except KeyError:
        raise ge_exceptions.ProfilerExecutionError(
            message=f'Unable to find value for parameter name "{fully_qualified_parameter_name}": key "{parameter_name_part}" was missing.'
        )


def get_batch_ids(data_context: DataContext, batch_request: BatchRequest) -> List[str]:
    datasource_name: str = batch_request.datasource_name
    batch_definitions: List[BatchDefinition] = data_context.get_datasource(
        datasource_name=datasource_name
    ).get_batch_definition_list_from_batch_request(batch_request=batch_request)
    return [batch_definition.id for batch_definition in batch_definitions]
