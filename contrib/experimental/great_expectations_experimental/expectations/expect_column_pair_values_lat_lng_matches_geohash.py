from typing import Optional

import geohash as gh
import pandas as pd

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import (
    ColumnPairMapExpectation,
    InvalidExpectationConfigurationError,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnPairMapMetricProvider,
    column_pair_condition_partial,
)


def compare(row):
    lat, lng = row[0]
    geohash = row[1]
    return gh.encode(lat, lng).startswith(geohash)


class ColumnPairValuesLatLngMatchesGeohash(ColumnPairMapMetricProvider):
    condition_metric_name = "column_pair_values.lat_lng_matches_geohash"

    # noinspection PyPep8Naming
    @column_pair_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_A, column_B, **kwargs):
        df = pd.DataFrame(column_A).join(column_B)
        return df.apply(lambda x: compare(x), axis=1)


class ExpectColumnPairValuesLatLngMatchesGeohash(ColumnPairMapExpectation):
    """
    Expect latlngs in column_A to match with geohashes in column_B.

    The more digits a geohash has, the smaller and more precise of an area it
    represents. When converting a latlng to a geohash, we are only asserting
    that it falls somewhere within the other geohash we're comparing it with.
    To verify this, we only need to make sure that they share their left-most
    digits. For example, dpz8 contains dpz80 (in addition to any other geohash
    that begins with "dpz8".
    """

    examples = [
        {
            "data": {
                "latlngs": [
                    (43.681640625, -79.27734375),
                    (43.681640620, -79.27734370),
                    (43.681640615, -79.27734385),
                ],
                "geohashes_same": ["dpz8", "dpz8", "dpz8"],
                "geohashes_different": ["d", "dpz8", "dpz87zzzzzzz"],
                "geohashes_incorrect": ["dpz8", "dpz7", "dpz6"],
            },
            "tests": [
                {
                    "title": "basic_positive",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_A": "latlngs", "column_B": "geohashes_same"},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_A": "latlngs", "column_B": "geohashes_incorrect"},
                    "out": {
                        "success": False,
                    },
                },
                {
                    "title": "basic_negative",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_A": "latlngs", "column_B": "geohashes_different"},
                    "out": {
                        "success": True,
                    },
                },
            ],
        }
    ]

    map_metric = "column_pair_values.lat_lng_matches_geohash"
    success_keys = (
        "column_A",
        "column_B",
        "ignore_row_if",
        "mostly",
    )
    default_kwarg_values = {
        "mostly": 1.0,
        "ignore_row_if": "both_values_are_missing"
    }

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "tags": [
            "geospatial",
            "hackathon-22",
            "multi-column expectation",
        ],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@chrisarnold91",  # Don't forget to add your github handle here!
        ],
        "requirements": ["python-geohash", "pandas"],
    }

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration
        try:
            assert (
                "column_A" in configuration.kwargs
                and "column_B" in configuration.kwargs
            ), "both columns must be provided"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))


if __name__ == "__main__":
    ExpectColumnPairValuesLatLngMatchesGeohash().print_diagnostic_checklist()
