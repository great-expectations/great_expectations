from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.types import SerializableDictDot

DOMAIN_KWARGS_PARAMETER_NAME: str = "domain_kwargs"
DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME: str = (
    f"$domain.{DOMAIN_KWARGS_PARAMETER_NAME}"
)
VARIABLES_KEY: str = "$variables."


def validate_fully_qualified_parameter_name(fully_qualified_parameter_name: str):
    if not fully_qualified_parameter_name.startswith("$"):
        raise ge_exceptions.ProfilerExecutionError(
            message=f"""Unable to get value for parameter name "{fully_qualified_parameter_name}" -- parameter \
names must start with $ (e.g., "${fully_qualified_parameter_name}").
"""
        )


@dataclass
class ParameterNode(SerializableDictDot):
    """
    ParameterNode is a node of a tree structure.

    Since the descendant nodes are of the same type as their parent node, then each descendant node is also a tree.

    Each node can optionally contain a bag of attribute name-value pairs ("parameter") and "details", with helpful
    information regarding how these attributes were obtained (tolerances, explanations, etc.).

    See the ParameterContainer documentation for examples of different parameter naming structures supported.

    Even though, typically, only the leaf nodes (characterized by having no descendants) contain attributes and details,
    intermediate nodes may also have these properties.
    """

    attributes: Optional[Dict[str, Any]] = None
    details: Optional[Dict[str, Any]] = None
    descendants: Optional[Dict[str, "ParameterNode"]] = None

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=asdict(self))


@dataclass
class ParameterContainer(SerializableDictDot):
    """
    ParameterContainer holds root nodes of tree structures, corresponding to fully-qualified parameter names.

    While all parameter names begin with the dollar sign character ("$"), a fully-qualified parameter name is a string,
    whose parts are delimited by the period character (".").

    As an example, suppose that the value of the attribute "max_num_conversion_attempts" is needed for certain
    processing operations.  However, there could be several attributes with this name, albeit in different contexts:
    $parameter.date_strings.tolerances.max_num_conversion_attempts
    $parameter.character.encodings.tolerances.max_num_conversion_attempts
    The fully-qualified parameter names disambiguate the same "leaf" attribute names occurring in multiple contexts.
    In the present example, the use of fully-qualified parameter names makes it clear that in one context,
    "max_num_conversion_attempts" refers to the operations on date/time, while in the other -- it applies to characters.

    $variables.false_positive_threshold
    $parameter.date_strings.yyyy_mm_dd_hh_mm_ss_tz_date_format
    $parameter.date_strings.yyyy_mm_dd_date_format
    $parameter.date_strings.mm_yyyy_dd_hh_mm_ss_tz_date_format
    $parameter.date_strings.mm_yyyy_dd_date_format
    $parameter.date_strings.tolerances.max_abs_error_time_milliseconds
    $parameter.date_strings.tolerances.max_num_conversion_attempts
    $parameter.tolerances.mostly
    $parameter.tolerances.financial.usd
    $mean
    $custom.lang.character_encodings

    The reason that ParameterContainer is needed is that each ParameterNode can point only to one tree structure,
    characterized by having a specific root-level ParameterNode object.  A root-level ParameterNode object corresponds
    to a set of fully-qualified parameter names that have the same first part (e.g., "parameter").  However, a Domain
    may utilize fully-qualified parameter names that have multiple first parts (i.e., from different "name spaces").
    The ParameterContainer maintains a dictionary that holds references to root-level ParameterNode objects for all
    parameter "name spaces" applicable to the given Domain (where the first part of all fully-qualified parameter names
    within the same "name space" serves as the dictionary key, and the root-level ParameterNode objects are the values).
    """

    parameter_nodes: Optional[Dict[str, ParameterNode]] = None

    def set_parameter_node(
        self, parameter_name_root: str, parameter_node: ParameterNode
    ):
        if self.parameter_nodes is None:
            self.parameter_nodes = {}
        self.parameter_nodes[parameter_name_root] = parameter_node

    def get_parameter_node(self, parameter_name_root: str) -> Optional[ParameterNode]:
        if self.parameter_nodes is None:
            return None
        return self.parameter_nodes.get(parameter_name_root)

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=asdict(self))


def build_parameter_container_for_variables(
    variables_configs: Dict[str, Any]
) -> ParameterContainer:
    """
    Build a ParameterContainer for all of the profiler config variables passed as key value pairs
    Args:
        variables_configs: Variable key: value pairs e.g. {"variable_name": variable_value, ...}

    Returns:
        ParameterContainer containing all variables
    """
    variable_config_key: str
    variable_config_value: Any
    parameter_values: Dict[str, Dict[str, Any]] = {}
    for variable_config_key, variable_config_value in variables_configs.items():
        variable_config_key = f"{VARIABLES_KEY}{variable_config_key}"
        parameter_values[variable_config_key] = {
            "value": variable_config_value,
        }

    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
    build_parameter_container(
        parameter_container=parameter_container, parameter_values=parameter_values
    )

    return parameter_container


def build_parameter_container(
    parameter_container: ParameterContainer,
    parameter_values: Dict[str, Dict[str, Any]],
):
    """
    Builds the ParameterNode trees, corresponding to the fully_qualified_parameter_name first-level keys.

    :param parameter_container initialized ParameterContainer for all ParameterNode trees
    :param parameter_values
    Example of required structure for "parameter_values" (matching the type hint in the method signature):
    {
        "$parameter.date_strings.tolerances.max_abs_error_time_milliseconds": {
            "value": 100, The key must be "value"; the actual value can be of Any type.
            "details": {  # This dictionary is Optional; however, if present, the key to it must be "details".
                "max_abs_error_time_milliseconds": {
                    "confidence": {  # Arbitrary dictionary key
                        "success_ratio": 1.0,  # Arbitrary entries
                        "comment": "matched template",  # Arbitrary entries
                    }
                },
            },
        },
        "$parameter.tolerances.mostly": {
            "value": 9.0e-1
            "details": None  # The "details" key is entirely Optional (it is exclusively for informational purposes).
        },
        ...
    }
    :return parameter_container holds the dictionary of ParameterNode objects corresponding to roots of parameter names

    This function loops through the supplied pairs of fully-qualified parameter names and their corresponding values
    (and any "details" metadata) and builds the tree under a single root-level ParameterNode object for a "name space".
    In particular, if any ParameterNode object in the tree (starting with the root-level ParameterNode object) already
    exists, it is reused; in other words, ParameterNode objects are unique per part of fully-qualified parameter names.
    """
    parameter_node: Optional[ParameterNode]
    fully_qualified_parameter_name: str
    parameter_value_details_dict: Dict[str, Any]
    fully_qualified_parameter_name_as_list: List[str]
    parameter_name_root: str
    parameter_value: Any
    parameter_details: Optional[Dict[str, Any]]
    for (
        fully_qualified_parameter_name,
        parameter_value_details_dict,
    ) in parameter_values.items():
        validate_fully_qualified_parameter_name(
            fully_qualified_parameter_name=fully_qualified_parameter_name
        )
        fully_qualified_parameter_name_as_list = fully_qualified_parameter_name[
            1:
        ].split(".")
        parameter_name_root = fully_qualified_parameter_name_as_list[0]
        parameter_value = parameter_value_details_dict["value"]
        # The details key is optional; in particular, it is commonly omitted for globally scoped "variables".
        parameter_details = parameter_value_details_dict.get("details")
        parameter_node = parameter_container.get_parameter_node(
            parameter_name_root=parameter_name_root
        )
        if parameter_node is None:
            parameter_node = ParameterNode(
                attributes=None, details=None, descendants=None
            )
            parameter_container.set_parameter_node(
                parameter_name_root=parameter_name_root, parameter_node=parameter_node
            )
        _build_parameter_node_tree_for_one_parameter(
            parameter_node=parameter_node,
            parameter_name_as_list=fully_qualified_parameter_name_as_list,
            parameter_value=parameter_value,
            details=parameter_details,
        )


def _build_parameter_node_tree_for_one_parameter(
    parameter_node: ParameterNode,
    parameter_name_as_list: List[str],
    parameter_value: Any,
    details: Optional[Any] = None,
):
    """
    Recursively builds a tree of ParameterNode objects, creating new ParameterNode objects parsimoniously (i.e., only if
    ParameterNode object, corresponding to a part of fully-qualified parameter names in a "name space" does not exist).
    :param parameter_node: root-level ParameterNode for the sub-tree, characterized by the first parameter name in list
    :param parameter_name_as_list: list of parts of a fully-qualified parameter name of sub-tree (or sub "name space")
    :param parameter_value: value pertaining to the last part of the fully-qualified parameter name ("leaf node")
    :param details: metadata/details pertaining to the last part of the fully-qualified parameter name ("leaf node")
    """
    parameter_name_part: str = parameter_name_as_list[0]

    # If the fully-qualified parameter name (or "name space") is still compound (i.e., not at "leaf node" / last part),
    # then build the sub-tree, creating the descendant ParameterNode (to hold the sub-tree), if no descendants exist.
    if len(parameter_name_as_list) > 1:
        if parameter_node.descendants is None:
            parameter_node.descendants = {}
        if parameter_name_part not in parameter_node.descendants:
            parameter_node.descendants[parameter_name_part] = ParameterNode(
                attributes=None, details=None, descendants=None
            )
        _build_parameter_node_tree_for_one_parameter(
            parameter_node=parameter_node.descendants[parameter_name_part],
            parameter_name_as_list=parameter_name_as_list[1:],
            parameter_value=parameter_value,
            details=details,
        )
    else:
        # If the fully-qualified parameter name (or "name space") is trivial (i.e., at "leaf node" / last part),
        # then store the supplied attribute name-value pairs as well as the metadata/details in the node's dictionaries.
        _set_parameter_node_attribute_name_value_pairs(
            parameter_node=parameter_node,
            attribute_name=parameter_name_part,
            attribute_value=parameter_value,
            details=details,
        )


def _set_parameter_node_attribute_name_value_pairs(
    parameter_node: ParameterNode,
    attribute_name: str,
    attribute_value: Any,
    details: Optional[Any] = None,
):
    if attribute_value is not None:
        if parameter_node.attributes is None:
            parameter_node.attributes = {}
        parameter_node.attributes[attribute_name] = attribute_value
    if details is not None:
        if parameter_node.details is None:
            parameter_node.details = {}
        parameter_node.details[attribute_name] = details


def get_parameter_value(
    fully_qualified_parameter_name: str,
    domain: Domain,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional[Any]:
    """
    Get the parameter value from the current rule state using the fully-qualified parameter name.
    A fully-qualified parameter name must be a dot-delimited string, or the name of a parameter (without the dots).
    Args
        :param fully_qualified_parameter_name: str -- A dot-separated string key starting with $ for fetching parameters
        :param domain: Union[Domain, List[Domain]] -- current Domain (or List[Domain]) of interest
        :param variables
        :param parameters
    :return: value
    """
    validate_fully_qualified_parameter_name(
        fully_qualified_parameter_name=fully_qualified_parameter_name
    )

    # Using "__getitem__" (bracket) notation instead of "__getattr__" (dot) notation in order to insure the
    # compatibility of field names (e.g., "domain_kwargs") with user-facing syntax (as governed by the value of the
    # DOMAIN_KWARGS_PARAMETER_NAME constant, which may change, requiring the same change to the field name).
    if fully_qualified_parameter_name == DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME:
        if domain:
            # Supports the "$domain.domain_kwargs" style syntax.
            return domain[DOMAIN_KWARGS_PARAMETER_NAME]
        return None

    if fully_qualified_parameter_name.startswith(
        DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME
    ):
        if domain and domain[DOMAIN_KWARGS_PARAMETER_NAME]:
            # Supports the "$domain.domain_kwargs.column" style syntax.
            return domain[DOMAIN_KWARGS_PARAMETER_NAME].get(
                fully_qualified_parameter_name[
                    (len(DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME) + 1) :
                ]
            )
        return None

    parameter_container: ParameterContainer

    if fully_qualified_parameter_name.startswith(VARIABLES_KEY):
        fully_qualified_parameter_name = fully_qualified_parameter_name[1:]
        parameter_container = variables
    else:
        fully_qualified_parameter_name = fully_qualified_parameter_name[1:]
        parameter_container = parameters[domain.id]

    fully_qualified_parameter_name_as_list: List[
        str
    ] = fully_qualified_parameter_name.split(".")

    if len(fully_qualified_parameter_name_as_list) == 0:
        return None

    return _get_parameter_value_from_parameter_container(
        fully_qualified_parameter_name=fully_qualified_parameter_name,
        fully_qualified_parameter_name_as_list=fully_qualified_parameter_name_as_list,
        parameter_container=parameter_container,
    )


def _get_parameter_value_from_parameter_container(
    fully_qualified_parameter_name: str,
    fully_qualified_parameter_name_as_list: List[str],
    parameter_container: ParameterContainer,
) -> Optional[Any]:
    parameter_node: Optional[ParameterNode] = parameter_container.get_parameter_node(
        parameter_name_root=fully_qualified_parameter_name_as_list[0]
    )
    if parameter_node is None:
        return None

    parameter_name_part: Optional[str] = None
    try:
        for parameter_name_part in fully_qualified_parameter_name_as_list:
            if (
                parameter_node.attributes
                and parameter_name_part in parameter_node.attributes
            ):
                return parameter_node.attributes[parameter_name_part]

            parameter_node = parameter_node.descendants[parameter_name_part]
    except KeyError:
        raise KeyError(
            f"""Unable to find value for parameter name "{fully_qualified_parameter_name}": Part \
"{parameter_name_part}" does not exist in fully-qualified parameter name.
"""
        )
