import copy
import random
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

import numpy as np

import great_expectations.exceptions as ge_exceptions
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.rule_based_profiler.parameter_builder.parameter_container import (
    ParameterContainer,
    get_parameter_value_by_fully_qualified_parameter_name,
)


def get_parameter_value_and_validate_return_type(
    domain: Domain,
    *,
    parameter_reference: Optional[Union[Any, str]] = None,
    expected_return_type: Optional[Union[type, tuple]] = None,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional[Any]:
    """
    This method allows for the parameter_reference to be specified as an object (literal, dict, any typed object, etc.)
    or as a fully-qualified parameter name.  In either case, it can optionally validate the type of the return value.
    """
    if isinstance(parameter_reference, dict):
        parameter_reference = dict(copy.deepcopy(parameter_reference))
    parameter_reference = get_parameter_value(
        domain=domain,
        parameter_reference=parameter_reference,
        variables=variables,
        parameters=parameters,
    )
    if expected_return_type is not None:
        if not isinstance(parameter_reference, expected_return_type):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""Argument "{parameter_reference}" must be of type "{str(expected_return_type)}" \
(value of type "{str(type(parameter_reference))}" was encountered).
"""
            )
    return parameter_reference


def get_parameter_value(
    domain: Domain,
    *,
    parameter_reference: Optional[Union[Any, str]] = None,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
) -> Optional[Any]:
    """
    This method allows for the parameter_reference to be specified as an object (literal, dict, any typed object, etc.)
    or as a fully-qualified parameter name.  Moreover, if the parameter_reference argument is an object of type "dict",
    it will recursively detect values using the fully-qualified parameter name format and evaluate them accordingly.
    """
    if isinstance(parameter_reference, dict):
        for key, value in parameter_reference.items():
            parameter_reference[key] = get_parameter_value(
                domain=domain,
                parameter_reference=value,
                variables=variables,
                parameters=parameters,
            )
    elif isinstance(parameter_reference, str) and parameter_reference.startswith("$"):
        parameter_reference = get_parameter_value_by_fully_qualified_parameter_name(
            fully_qualified_parameter_name=parameter_reference,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        if isinstance(parameter_reference, dict):
            for key, value in parameter_reference.items():
                parameter_reference[key] = get_parameter_value(
                    domain=domain,
                    parameter_reference=value,
                    variables=variables,
                    parameters=parameters,
                )
    return parameter_reference


class BootstrapStandardErrorEstimator(ABC):
    def __init__(
        self,
        sample_size_n: int,
    ):
        """
        # TODO: <Alex>ALEX -- Docstring</Alex>
        """
        self._sample_size_n = sample_size_n

    # sample_mean = []
    # for i in range(50):
    #     y = random.sample(x.tolist(), 4)
    #     avg = np.mean(y)
    #     sample_mean.append(avg)
    #
    # print(np.mean(sample_mean))
    """
>>> import scipy.stats
>>> scipy.stats.norm.ppf(0.975)
1.959963984540054
>>> scipy.stats.norm.ppf(1.0)
inf
>>> scipy.stats.norm.ppf(0.0)
-inf
>>> scipy.stats.norm.ppf(0.5)
0.0
>>> """

    def generate_random_sample_indexes(
        self,
    ) -> List[int]:
        permutation: List[int] = np.arange(self.sample_size_n)
        return random.choices(permutation, k=self.sample_size_n)

    @abstractmethod
    def compute_statistic(self) -> Union[float, np.float32, np.float64]:
        pass

    @property
    def sample_size_n(self) -> int:
        return self._sample_size_n
