import logging

from great_expectations.core import IDDict
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import SerializableDotDict

logger = logging.getLogger(__name__)


class Attributes(SerializableDotDict, IDDict):
    """
    This class generalizes dictionary in order to hold generic attributes with unique ID.
    """

    def to_dict(self) -> dict:
        return dict(self)

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=self.to_dict())
