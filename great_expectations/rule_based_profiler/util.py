import copy
import random
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

import numpy as np
import scipy.stats

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


class SingleNumericStatisticCalculator(ABC):
    @property
    @abstractmethod
    def sample_identifiers(
        self,
    ) -> List[Union[bytes, str, int, float, complex, tuple, frozenset]]:
        """
        :return: List of Hashable objects
        """
        pass

    @abstractmethod
    def generate_distribution_sample(
        self,
        randomized_sample_identifiers: List[
            Union[
                bytes,
                str,
                int,
                float,
                complex,
                tuple,
                frozenset,
            ]
        ],
    ) -> Union[
        np.ndarray, List[Union[int, np.int32, np.int64, float, np.float32, np.float64]]
    ]:
        """
        Computes numeric statistic from unique identifiers of data samples (a unique identifier must be hashable).
        :parameter: randomized_sample_identifiers -- List of Hashable objects
        :return: np.float64
        """
        pass

    @abstractmethod
    def compute_numeric_statistic(
        self,
        randomized_sample_identifiers: List[
            Union[
                bytes,
                str,
                int,
                float,
                complex,
                tuple,
                frozenset,
            ]
        ],
    ) -> np.float64:
        """
        Computes numeric statistic from unique identifiers of data samples (a unique identifier must be hashable).
        :parameter: randomized_sample_identifiers -- List of Hashable objects
        :return: np.float64
        """
        pass


class BootstrappedStandardErrorOptimizationBasedEstimator:
    def __init__(
        self,
        statistic_calculator: SingleNumericStatisticCalculator,
        sample_size: int,
        bootstrapped_statistic_deviation_bound: Optional[float] = 1.0e-1,
        prob_bootstrapped_statistic_deviation_outside_bound: Optional[float] = 5.0e-2,
    ):
        """
        # TODO: <Alex>ALEX -- Docstring</Alex>
        """
        self._statistic_calculator = statistic_calculator
        if sample_size < 2:
            raise ValueError(
                f"""Argument "sample_size" in {self.__class__.__name__} must be an integer greater than 1 \
(the value {sample_size} was encountered).
"""
            )
        self._sample_size = sample_size

        self._bootstrapped_statistic_deviation_bound = (
            bootstrapped_statistic_deviation_bound
        )
        self._prob_bootstrapped_statistic_deviation_outside_bound = (
            prob_bootstrapped_statistic_deviation_outside_bound
        )

        self._optimal_num_bootstrap_samples_estimations = []

    def compute_bootstrapped_statistic_samples(self) -> np.ndarray:
        optimal_num_bootstrap_samples: int = (
            self._estimate_optimal_num_bootstrap_samples()
        )
        bootstrap_samples: np.ndarray = self._generate_bootstrap_samples(
            num_bootstrap_samples=optimal_num_bootstrap_samples
        )
        return bootstrap_samples

    def _estimate_optimal_num_bootstrap_samples(
        self,
    ) -> int:
        optimal_num_bootstrap_samples: int = self._estimate_num_bootstrap_samples(
            bootstrap_samples=None
        )
        self._optimal_num_bootstrap_samples_estimations.append(
            optimal_num_bootstrap_samples
        )

        previous_max_optimal_num_bootstrap_samples: int = 0
        current_max_optimal_num_bootstrap_samples: int = max(
            self._optimal_num_bootstrap_samples_estimations
        )

        while (
            current_max_optimal_num_bootstrap_samples
            > previous_max_optimal_num_bootstrap_samples
        ):
            bootstrap_samples = self._generate_bootstrap_samples(
                num_bootstrap_samples=optimal_num_bootstrap_samples
            )
            optimal_num_bootstrap_samples = self._estimate_num_bootstrap_samples(
                bootstrap_samples=bootstrap_samples
            )
            self._optimal_num_bootstrap_samples_estimations.append(
                optimal_num_bootstrap_samples
            )
            previous_max_optimal_num_bootstrap_samples = (
                current_max_optimal_num_bootstrap_samples
            )
            current_max_optimal_num_bootstrap_samples = max(
                self._optimal_num_bootstrap_samples_estimations
            )

        return current_max_optimal_num_bootstrap_samples

    def _generate_bootstrap_samples(self, num_bootstrap_samples: int) -> np.ndarray:
        idx: int
        # noinspection PyUnusedLocal
        bootstrap_samples: Union[
            np.ndarray,
            List[Union[float, np.float32, np.float64]],
        ] = [
            self._compute_statistic_for_random_sample()
            for idx in range(num_bootstrap_samples)
        ]
        bootstrap_samples = np.array(bootstrap_samples, dtype=np.float64)
        return bootstrap_samples

    def _estimate_num_bootstrap_samples(
        self, bootstrap_samples: Optional[np.ndarray] = None
    ) -> int:
        quantile_complement_prob_outside_bound_divided_by_2: np.float64 = (
            scipy.stats.norm.ppf(
                1.0 - self._prob_bootstrapped_statistic_deviation_outside_bound
            )
        )

        excess_kurtosis: Optional[np.float64]
        if bootstrap_samples is None:
            excess_kurtosis = np.float64(0.0)
        else:
            excess_kurtosis = self._bootstrapped_sample_excess_kurtosis(
                bootstrap_samples=bootstrap_samples
            )

        statistic_deviation_standard_variance: np.float64 = (
            self._bootstrapped_statistic_deviation_standard_variance(
                excess_kurtosis=excess_kurtosis
            )
        )

        bootstrap_samples_fractional: np.float64 = np.float64(
            quantile_complement_prob_outside_bound_divided_by_2
            * statistic_deviation_standard_variance
            / (
                self._bootstrapped_statistic_deviation_bound
                * self._bootstrapped_statistic_deviation_bound
            )
        )
        bootstrap_samples: int = round(bootstrap_samples_fractional)

        return bootstrap_samples

    def _generate_random_sample_indexes(
        self,
    ) -> List[int]:
        permutation: List[int] = np.arange(self._sample_size)
        return random.choices(permutation, k=self._sample_size)

    def _compute_statistic_for_random_sample(self) -> np.float64:
        random_sample_indexes: List[int] = self._generate_random_sample_indexes()
        original_data_sample_ids: List[
            Union[
                bytes,
                str,
                int,
                float,
                complex,
                tuple,
                frozenset,
            ]
        ] = self._statistic_calculator.sample_identifiers
        idx: int
        randomized_sample_identifiers: List[
            Union[
                bytes,
                str,
                int,
                float,
                complex,
                tuple,
                frozenset,
            ]
        ] = [original_data_sample_ids[idx] for idx in random_sample_indexes]
        computed_sample_statistic: np.float64 = (
            self._statistic_calculator.compute_numeric_statistic(
                randomized_sample_identifiers=randomized_sample_identifiers
            )
        )
        return computed_sample_statistic

    @staticmethod
    def _bootstrapped_statistic_deviation_standard_variance(
        excess_kurtosis: Optional[np.float64] = 0.0,
    ) -> np.float64:
        return np.float64((2.0 + excess_kurtosis) / 4.0)

    def _bootstrapped_sample_excess_kurtosis(
        self,
        bootstrap_samples: np.ndarray,
    ) -> np.float64:
        return np.float64(
            self._bootstrapped_sample_kurtosis(bootstrap_samples=bootstrap_samples)
            - 3.0
        )

    def _bootstrapped_sample_kurtosis(
        self,
        bootstrap_samples: np.ndarray,
    ) -> np.float64:
        num_bootstrap_samples: int = bootstrap_samples.size
        if num_bootstrap_samples < 2:
            raise ValueError(
                f"""Number of bootstrap samples in {self.__class__.__name__} must be an integer greater than 1 \
(the value {num_bootstrap_samples} was encountered).
"""
            )
        sample_mean: np.float64 = self._bootstrapped_sample_mean(
            bootstrap_samples=num_bootstrap_samples
        )
        bootstrap_samples_mean_removed: np.ndarray = bootstrap_samples - sample_mean
        bootstrap_samples_mean_removed_power_4: np.ndarray = np.power(
            bootstrap_samples_mean_removed, 4
        )
        sample_kurtosis: np.float64 = np.sum(bootstrap_samples_mean_removed_power_4) / (
            self._bootstrapped_sample_standard_variance(
                bootstrap_samples=bootstrap_samples
            )
            * self._bootstrapped_sample_standard_variance(
                bootstrap_samples=bootstrap_samples
            )
        )
        return sample_kurtosis

    def _bootstrapped_sample_standard_variance(
        self,
        bootstrap_samples: np.ndarray,
    ) -> np.float64:
        num_bootstrap_samples: int = bootstrap_samples.size
        if num_bootstrap_samples < 2:
            raise ValueError(
                f"""Number of bootstrap samples in {self.__class__.__name__} must be an integer greater than 1 \
(the value {num_bootstrap_samples} was encountered).
"""
            )

        sample_variance: np.float64 = self._bootstrapped_sample_variance(
            bootstrap_samples=num_bootstrap_samples
        )
        sample_standard_variance: np.float64 = np.float64(
            num_bootstrap_samples * sample_variance / (num_bootstrap_samples - 1)
        )
        return sample_standard_variance

    @staticmethod
    def _bootstrapped_sample_variance(
        bootstrap_samples: np.ndarray,
    ) -> np.float64:
        sample_variance: Union[np.ndarray, np.float64] = np.var(bootstrap_samples)
        return np.float64(sample_variance)

    @staticmethod
    def _bootstrapped_sample_mean(
        bootstrap_samples: np.ndarray,
    ) -> np.float64:
        sample_mean: Union[np.ndarray, np.float64] = np.mean(bootstrap_samples)
        return np.float64(sample_mean)
