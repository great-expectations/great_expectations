from __future__ import annotations

import logging
from typing import Final

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.metrics.column_aggregate_metric_provider import (
    ColumnAggregateMetricProvider,
    column_aggregate_value,
)
from great_expectations.expectations.metrics.util import (
    is_valid_continuous_distribution_object,
)

logger = logging.getLogger(__name__)


import numpy as np
from scipy import stats

NP_RANDOM_GENERATOR: Final = np.random.default_rng()


class ColumnBootstrappedKSTestPValue(ColumnAggregateMetricProvider):
    """MetricProvider Class for Aggregate Standard Deviation metric"""

    metric_name = "column.bootstrapped_ks_test_p_value"
    value_keys = ("distribution_object", "p", "bootstrap_sample", "bootstrap_sample_size")

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(  # noqa: C901, PLR0913
        cls,
        column,
        distribution_object=None,
        p=0.05,
        bootstrap_samples=None,
        bootstrap_sample_size=None,
        **kwargs,
    ):
        if not is_valid_continuous_distribution_object(distribution_object):
            raise ValueError("Invalid continuous distribution object.")  # noqa: TRY003

        # TODO: consider changing this into a check that tail_weights does not exist exclusively, by moving this check into is_valid_continuous_distribution_object  # noqa: E501
        if (distribution_object["bins"][0] == -np.inf) or (
            distribution_object["bins"][-1] == np.inf
        ):
            raise ValueError("Partition endpoints must be finite.")  # noqa: TRY003

        if (
            "tail_weights" in distribution_object
            and np.sum(distribution_object["tail_weights"]) > 0
        ):
            raise ValueError("Partition cannot have tail weights -- endpoints must be finite.")  # noqa: TRY003

        test_cdf = np.append(np.array([0]), np.cumsum(distribution_object["weights"]))

        def estimated_cdf(x):
            return np.interp(x, distribution_object["bins"], test_cdf)

        if bootstrap_samples is None:
            bootstrap_samples = 1000

        if bootstrap_sample_size is None:
            # Sampling too many elements (or not bootstrapping) will make the test too sensitive to the fact that we've  # noqa: E501
            # compressed via a distribution.

            # Sampling too few elements will make the test insensitive to significant differences, especially  # noqa: E501
            # for nonoverlapping ranges.
            bootstrap_sample_size = len(distribution_object["weights"]) * 2

        results = [
            stats.kstest(
                NP_RANDOM_GENERATOR.choice(column, size=bootstrap_sample_size),
                estimated_cdf,
            )[1]
            for _ in range(bootstrap_samples)
        ]

        test_result = (1 + sum(x >= p for x in results)) / (bootstrap_samples + 1)

        hist, _bin_edges = np.histogram(column, distribution_object["bins"])
        below_distribution = len(np.where(column < distribution_object["bins"][0])[0])
        above_distribution = len(np.where(column > distribution_object["bins"][-1])[0])

        # Expand observed distribution to report, if necessary
        if below_distribution > 0 and above_distribution > 0:
            observed_bins = [np.min(column)] + distribution_object["bins"] + [np.max(column)]
            observed_weights = np.concatenate(
                ([below_distribution], hist, [above_distribution])
            ) / len(column)
        elif below_distribution > 0:
            observed_bins = [np.min(column)] + distribution_object["bins"]
            observed_weights = np.concatenate(([below_distribution], hist)) / len(column)
        elif above_distribution > 0:
            observed_bins = distribution_object["bins"] + [np.max(column)]
            observed_weights = np.concatenate((hist, [above_distribution])) / len(column)
        else:
            observed_bins = distribution_object["bins"]
            observed_weights = hist / len(column)

        observed_cdf_values = np.cumsum(observed_weights)

        # TODO: How should this metric's return_obj be structured?
        return_obj = {
            "observed_value": test_result,
            "details": {
                "bootstrap_samples": bootstrap_samples,
                "bootstrap_sample_size": bootstrap_sample_size,
                "observed_distribution": {
                    "bins": observed_bins,
                    "weights": observed_weights.tolist(),
                },
                "expected_distribution": {
                    "bins": distribution_object["bins"],
                    "weights": distribution_object["weights"],
                },
                "observed_cdf": {
                    "x": observed_bins,
                    "cdf_values": [0] + observed_cdf_values.tolist(),
                },
                "expected_cdf": {
                    "x": distribution_object["bins"],
                    "cdf_values": test_cdf.tolist(),
                },
            },
        }

        return return_obj
