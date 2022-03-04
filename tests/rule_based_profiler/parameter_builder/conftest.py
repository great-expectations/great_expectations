from numbers import Number
from typing import Dict, Optional

import numpy as np
import pandas as pd
import pytest
import scipy.stats as stats


def _generate_distribution_parameters(
    false_positive_rate: np.float64,
) -> Dict[str, Dict[str, Number]]:
    normal_mean: int = 5000
    normal_stdev: int = 1000
    normal_lower_quantile: Number = stats.norm.ppf(
        q=false_positive_rate / 2, loc=normal_mean, scale=normal_stdev
    )
    normal_upper_quantile: Number = stats.norm.ppf(
        q=1 - false_positive_rate / 2, loc=normal_mean, scale=normal_stdev
    )

    uniform_lower_bound: int = 4000
    uniform_scale: int = 6000
    uniform_lower_quantile: Number = stats.uniform.ppf(
        q=false_positive_rate / 2, loc=uniform_lower_bound, scale=uniform_scale
    )
    uniform_upper_quantile: Number = stats.uniform.ppf(
        q=1 - false_positive_rate / 2, loc=uniform_lower_bound, scale=uniform_scale
    )

    bimodal_mean_1: int = 4000
    bimodal_stdev_1: int = 500
    bimodal_mean_2: int = 6000
    bimodal_stdev_2: int = 500
    bimodal_approximation = np.concatenate(
        [
            stats.norm.rvs(
                loc=bimodal_mean_1,
                scale=bimodal_stdev_1,
                size=5000,
            ),
            stats.norm.rvs(
                loc=bimodal_mean_2,
                scale=bimodal_stdev_2,
                size=5000,
            ),
        ]
    )
    bimodal_lower_quantile = np.quantile(
        a=bimodal_approximation, q=false_positive_rate / 2
    )
    bimodal_upper_quantile = np.quantile(
        a=bimodal_approximation, q=1 - false_positive_rate / 2
    )

    exponential_shape: int = 1.5
    exponential_lower_bound: int = 5000
    exponential_scale: int = 1000
    exponential_lower_quantile: Number = stats.gamma.ppf(
        q=false_positive_rate / 2,
        a=exponential_shape,
        loc=exponential_lower_bound,
        scale=exponential_scale,
    )
    exponential_upper_quantile: Number = stats.gamma.ppf(
        q=1 - false_positive_rate / 2,
        a=exponential_shape,
        loc=exponential_lower_bound,
        scale=exponential_scale,
    )

    return {
        "normal": {
            "mean": normal_mean,
            "stdev": normal_stdev,
            "lower_quantile": normal_lower_quantile,
            "upper_quantile": normal_upper_quantile,
        },
        "uniform": {
            "lower_bound": uniform_lower_bound,
            "scale": uniform_scale,
            "lower_quantile": uniform_lower_quantile,
            "upper_quantile": uniform_upper_quantile,
        },
        "bimodal": {
            "mean_1": bimodal_mean_1,
            "stdev_1": bimodal_stdev_1,
            "mean_2": bimodal_mean_2,
            "stdev_2": bimodal_stdev_2,
            "lower_quantile": bimodal_lower_quantile,
            "upper_quantile": bimodal_upper_quantile,
        },
        "exponential": {
            "shape": exponential_shape,
            "lower_bound": exponential_lower_bound,
            "scale": exponential_scale,
            "lower_quantile": exponential_lower_quantile,
            "upper_quantile": exponential_upper_quantile,
        },
    }


def _generate_distribution_samples(
    distribution_parameters: Dict[str, Dict[str, Number]], size: Optional[int] = 36
) -> pd.DataFrame:
    data: Dict[str, np.ndarray] = {
        "normal": np.around(
            stats.norm.rvs(
                loc=distribution_parameters["normal"]["mean"],
                scale=distribution_parameters["normal"]["stdev"],
                size=size,
            )
        ),
        "uniform": np.around(
            stats.uniform.rvs(
                loc=distribution_parameters["uniform"]["lower_bound"],
                scale=distribution_parameters["uniform"]["scale"],
                size=size,
            )
        ),
        "bimodal": np.around(
            np.concatenate(
                [
                    stats.norm.rvs(
                        loc=distribution_parameters["bimodal"]["mean_1"],
                        scale=distribution_parameters["bimodal"]["stdev_1"],
                        size=size // 2,
                    ),
                    stats.norm.rvs(
                        loc=distribution_parameters["bimodal"]["mean_2"],
                        scale=distribution_parameters["bimodal"]["stdev_2"],
                        size=size // 2,
                    ),
                ]
            )
        ),
        "exponential": np.around(
            stats.gamma.rvs(
                a=distribution_parameters["exponential"]["shape"],
                loc=distribution_parameters["exponential"]["lower_bound"],
                scale=distribution_parameters["exponential"]["scale"],
                size=size,
            )
        ),
    }
    return pd.DataFrame(data)


@pytest.fixture
def bootstrap_distribution_parameters_and_1000_samples_with_01_false_positive():
    false_positive_rate: np.float64 = np.float64(0.01)
    distribution_parameters: Dict[
        str, Dict[str, Number]
    ] = _generate_distribution_parameters(false_positive_rate=false_positive_rate)
    distribution_samples: pd.DataFrame = _generate_distribution_samples(
        distribution_parameters=distribution_parameters, size=1000
    )
    return {
        "false_positive_rate": false_positive_rate,
        "distribution_parameters": distribution_parameters,
        "distribution_samples": distribution_samples,
    }


@pytest.fixture
def bootstrap_distribution_parameters_and_36_samples_with_01_false_positive():
    false_positive_rate: np.float64 = np.float64(0.01)
    distribution_parameters: Dict[
        str, Dict[str, Number]
    ] = _generate_distribution_parameters(false_positive_rate=false_positive_rate)
    distribution_samples: pd.DataFrame = _generate_distribution_samples(
        distribution_parameters=distribution_parameters, size=36
    )
    return {
        "false_positive_rate": false_positive_rate,
        "distribution_parameters": distribution_parameters,
        "distribution_samples": distribution_samples,
    }


@pytest.fixture
def bootstrap_distribution_parameters_and_5_samples_with_01_false_positive():
    false_positive_rate: np.float64 = np.float64(0.01)
    distribution_parameters: Dict[
        str, Dict[str, Number]
    ] = _generate_distribution_parameters(false_positive_rate=false_positive_rate)
    distribution_samples: pd.DataFrame = _generate_distribution_samples(
        distribution_parameters=distribution_parameters, size=36
    )
    return {
        "false_positive_rate": false_positive_rate,
        "distribution_parameters": distribution_parameters,
        "distribution_samples": distribution_samples,
    }
