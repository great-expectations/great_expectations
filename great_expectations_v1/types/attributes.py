from __future__ import annotations

import logging

from great_expectations_v1.compatibility.typing_extensions import override
from great_expectations_v1.core import IDDict
from great_expectations_v1.core.util import convert_to_json_serializable
from great_expectations_v1.types import SerializableDotDict

logger = logging.getLogger(__name__)


class Attributes(SerializableDotDict, IDDict):
    """
    This class generalizes dictionary in order to hold generic attributes with unique ID.
    """

    def to_dict(self) -> dict:
        return dict(self)

    @override
    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=self.to_dict())
