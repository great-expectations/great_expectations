import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional

import numpy as np

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.rule_based_profiler.domain import Domain
from great_expectations.rule_based_profiler.numeric_range_estimation_result import (
    NumericRangeEstimationResult,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
)
from great_expectations.types import SerializableDictDot
from great_expectations.types.attributes import Attributes

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class NumericRangeEstimator(ABC, SerializableDictDot):
    """
    # TODO: <Alex>ALEX</Alex>
    """

    def __init__(
        self,
        name: str,
        configuration: Optional[Attributes] = None,
    ) -> None:
        """
        Args:
        # TODO: <Alex>ALEX</Alex>
        """
        self._name = name
        self._configuration = configuration

    @property
    def name(self) -> str:
        return self._name

    @property
    def configuration(self) -> Optional[Attributes]:
        return self._configuration

    @configuration.setter
    def configuration(self, value: Optional[Attributes]) -> None:
        self._configuration = value

    @abstractmethod
    def get_numeric_range_estimate(
        self,
        metric_values: np.ndarray,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> NumericRangeEstimationResult:
        """
        Args:
        # TODO: <Alex>ALEX</Alex>
        """
        pass

    def to_dict(self) -> dict:
        """
        Returns dictionary equivalent of this object.
        """
        dict_obj: dict = self._configuration.to_dict()
        dict_obj.update({"name": self._name})
        return dict_obj

    def to_json_dict(self) -> dict:
        """
        Returns JSON dictionary equivalent of this object.
        """
        return convert_to_json_serializable(data=self.to_dict())
