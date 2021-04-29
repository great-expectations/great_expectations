from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import SerializableDictDot

DOMAIN_KWARGS_PARAMETER_NAME: str = "domain_kwargs"
DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME: str = (
    f"$domain.{DOMAIN_KWARGS_PARAMETER_NAME}"
)
VARIABLES_KEY: str = "$variables."


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
    ParameterContainer holds root nodes of tree structures, corresponding to fully qualified parameter names.

    $parameter.date_strings.yyyy_mm_dd_hh_mm_ss_tz_date_format
    $parameter.date_strings.yyyy_mm_dd_date_format
    $parameter.date_strings.mm_yyyy_dd_hh_mm_ss_tz_date_format
    $parameter.date_strings.mm_yyyy_dd_date_format
    $parameter.date_strings.tolerances.max_abs_error_time_milliseconds
    $parameter.date_strings.tolerances.max_num_conversion_attempts
    $parameter.tolerances.mostly
    $mean
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


def build_parameter_container(
    parameter_values: Dict[str, Dict[str, Any]]
) -> ParameterContainer:
    """
    Builds the ParameterNode trees, corresponding to the fully_qualified_parameter_name first-level keys.

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
    """
    parameter_container: ParameterContainer = ParameterContainer(parameter_nodes=None)
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
        parameter_details = parameter_value_details_dict["details"]
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

    return parameter_container


def _build_parameter_node_tree_for_one_parameter(
    parameter_node: ParameterNode,
    parameter_name_as_list: List[str],
    parameter_value: Any,
    details: Optional[Any] = None,
) -> None:
    parameter_name_part: str = parameter_name_as_list[0]

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
        _assign_parameter_node_atrribute_name_value_pairs(
            parameter_node=parameter_node,
            attribute_name=parameter_name_part,
            attribute_value=parameter_value,
            details=details,
        )


def _assign_parameter_node_atrribute_name_value_pairs(
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


def validate_fully_qualified_parameter_name(fully_qualified_parameter_name: str):
    if not fully_qualified_parameter_name.startswith("$"):
        raise ge_exceptions.ProfilerExecutionError(
            message=f"""Unable to get value for parameter name "{fully_qualified_parameter_name}" -- parameter \
names must start with $ (e.g., "${fully_qualified_parameter_name}").
"""
        )
