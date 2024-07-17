from __future__ import annotations

import abc
import logging
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional

from great_expectations.core.profiler_types_mapping import ProfilerTypeMapping

if TYPE_CHECKING:
    from great_expectations.core.expectation_suite import ExpectationSuite

logger = logging.getLogger(__name__)


class OrderedEnum(Enum):
    def __ge__(self, other):
        if self.__class__ is other.__class__:
            return self.value >= other.value
        return NotImplemented

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self.value > other.value
        return NotImplemented

    def __le__(self, other):
        if self.__class__ is other.__class__:
            return self.value <= other.value
        return NotImplemented

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


class OrderedProfilerCardinality(OrderedEnum):
    NONE = 0
    ONE = 1
    TWO = 2
    VERY_FEW = 3
    FEW = 4
    MANY = 5
    VERY_MANY = 6
    UNIQUE = 7

    @classmethod
    def get_basic_column_cardinality(cls, num_unique=0, pct_unique=0) -> OrderedProfilerCardinality:
        """
        Takes the number and percentage of unique values in a column and returns the column cardinality.
        If you are unexpectedly returning a cardinality of "None", ensure that you are passing in values for both
        num_unique and pct_unique.

        Args:
            num_unique: The number of unique values in a column
            pct_unique: The percentage of unique values in a column

        Returns:
            The column cardinality
        """  # noqa: E501
        if pct_unique == 1.0:
            cardinality = cls.UNIQUE
        elif num_unique == 1:
            cardinality = cls.ONE
        elif num_unique == 2:  # noqa: PLR2004
            cardinality = cls.TWO
        elif 0 < num_unique < 20:  # noqa: PLR2004
            cardinality = cls.VERY_FEW
        elif 0 < num_unique < 60:  # noqa: PLR2004
            cardinality = cls.FEW
        elif num_unique is None or num_unique == 0 or pct_unique is None:
            cardinality = cls.NONE
        elif pct_unique > 0.1:  # noqa: PLR2004
            cardinality = cls.VERY_MANY
        else:
            cardinality = cls.MANY
        return cardinality


class ProfilerDataType(Enum):
    """Useful data types for building profilers."""

    INT = "int"
    FLOAT = "float"
    NUMERIC = "numeric"
    STRING = "string"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    UNKNOWN = "unknown"


class ProfilerCardinality(Enum):
    """Useful cardinality categories for building profilers."""

    NONE = "none"
    ONE = "one"
    TWO = "two"
    FEW = "few"
    VERY_FEW = "very few"
    MANY = "many"
    VERY_MANY = "very many"
    UNIQUE = "unique"


profiler_data_types_with_mapping = {
    "INT": list(ProfilerTypeMapping.INT_TYPE_NAMES),
    "FLOAT": list(ProfilerTypeMapping.FLOAT_TYPE_NAMES),
    "NUMERIC": (
        list(ProfilerTypeMapping.INT_TYPE_NAMES) + list(ProfilerTypeMapping.FLOAT_TYPE_NAMES)
    ),
    "STRING": list(ProfilerTypeMapping.STRING_TYPE_NAMES),
    "BOOLEAN": list(ProfilerTypeMapping.BOOLEAN_TYPE_NAMES),
    "DATETIME": list(ProfilerTypeMapping.DATETIME_TYPE_NAMES),
    "UNKNOWN": ["unknown"],
}


class ProfilerSemanticTypes(Enum):
    DATETIME = "DATETIME"
    NUMERIC = "NUMERIC"
    STRING = "STRING"
    VALUE_SET = "VALUE_SET"
    BOOLEAN = "BOOLEAN"
    OTHER = "OTHER"


class Profiler(metaclass=abc.ABCMeta):
    """
    Profiler creates suites from various sources of truth.

    These sources of truth can be data or non-data sources such as DDLs.

    When implementing a Profiler ensure that you:
    - Implement a . _profile() method
    - Optionally implement .validate() method that verifies you are running on the right
      kind of object. You should raise an appropriate Exception if the object is not valid.
    """

    def __init__(self, configuration: Optional[dict] = None) -> None:
        self.configuration = configuration

    def validate(  # noqa: B027  # empty-method-without-abstract-decorator
        self, item_to_validate: Any
    ) -> None:
        """Raise an exception if `item_to_validate` cannot be profiled.

        Args:
            item_to_validate: The item to validate.
        """
        pass

    def profile(self, item_to_profile: Any, suite_name: Optional[str] = None) -> ExpectationSuite:
        self.validate(item_to_profile)
        expectation_suite = self._profile(item_to_profile, suite_name=suite_name)
        return expectation_suite

    @abc.abstractmethod
    def _profile(self, item_to_profile: Any, suite_name: Optional[str] = None) -> ExpectationSuite:
        pass
