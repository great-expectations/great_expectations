from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Union

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import SerializableDictDot


@dataclass
class ParameterContainer(SerializableDictDot):
    """
    ParameterContainer is a node of a tree structure.

    Since the descendant nodes are of the same type as their parent node, then each descendant node is also a tree.

    Each node can optionally contain a bag of attribute name-value pairs ("parameter") and "details", with helpful
    information regarding how the parameters were obtained (tolerances, explanations, etc.).

    $parameter.date_strings.yyyy_mm_dd_date_format
    $parameter.date_strings.mm_yyyy_dd_date_format
    $parameter.date_strings.tolerances.max_abs_error_time_milliseconds
    $parameter.date_strings.tolerances.max_num_conversion_attempts
    $parameter.tolerances.mostly

    Typically, only the leaf nodes (characterized by having no descendants) contain attributes and details.
    Nevertheless, intermediate nodes may also have these properties.
    """

    parameters: Optional[Dict[str, Any]] = None
    details: Optional[Dict[str, Union[str, dict]]] = None
    descendants: Optional[Dict[str, "ParameterContainer"]] = None

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=asdict(self))
