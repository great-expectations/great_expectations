import numpy as np

from great_expectations.dataset import Dataset, SparkDFDataset


class CustomSparkDFDataset(SparkDFDataset):
    _data_asset_type = "CustomSparkDFDataset"

    def get_column_approx_quantiles(self, column, quantiles, tolerance=0.2):
        return self.spark_df.approxQuantile(column, list(quantiles), tolerance)

    @Dataset.column_aggregate_expectation
    def expect_column_approx_quantile_values_to_be_between(
        self,
        column,
        quantile_ranges,
        tolerance=0.2,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        quantiles = quantile_ranges["quantiles"]
        quantile_value_ranges = quantile_ranges["value_ranges"]
        if len(quantiles) != len(quantile_value_ranges):
            raise ValueError(
                "quntile_values and quantiles must have the same number of elements"
            )

        quantile_vals = self.get_column_approx_quantiles(
            column, tuple(quantiles), tolerance
        )
        # We explicitly allow "None" to be interpreted as +/- infinity
        comparison_quantile_ranges = [
            [lower_bound or -np.inf, upper_bound or np.inf]
            for (lower_bound, upper_bound) in quantile_value_ranges
        ]
        success_details = [
            range_[0] <= quantile_vals[idx] <= range_[1]
            for idx, range_ in enumerate(comparison_quantile_ranges)
        ]

        return {
            "success": np.all(success_details),
            "result": {
                "observed_value": {"quantiles": quantiles, "values": quantile_vals},
                "details": {"success_details": success_details},
            },
        }
