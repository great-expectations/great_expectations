from typing import ClassVar, List, Literal, Tuple

import geohash as gh
import pandas as pd

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import (
    ColumnPairMapExpectation,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnPairMapMetricProvider,
    column_pair_condition_partial,
)


def compare(row):
    """"""
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
    """Expect latlngs in column A to match with geohashes in column B.

    The more digits a geohash has, the smaller and more precise of an area it represents.
    When converting a latlng to a geohash, we are only asserting that it falls somewhere within the other geohash
    we're comparing it with.  To verify this, we only need to make sure that they share their left-most digits.
    For example, dpz8 contains dpz80 (in addition to any other geohash that begins with "dpz8".

    expect_column_pair_values_lat_lng_matches_geohash is a \
    [Column Pair Map Expectation](https://docs.greatexpectations.io/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_column_pair_map_expectations)

    Args:
        column_A (str): \
            The first column name
        column_B (str): \
            The second column name

    Keyword Args:
        ignore_row_if (str): \
            "all_values_are_missing", "any_value_is_missing", "never"
        mostly (None or a float between 0 and 1): \
            Successful if at least mostly fraction of values match the expectation. \
            For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly).

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).
    """

    ignore_row_if: Literal["both_values_are_missing", "either_value_is_missing", "neither"] = (
        "both_values_are_missing"
    )

    examples: ClassVar[List[dict]] = [
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

    map_metric: ClassVar[str] = "column_pair_values.lat_lng_matches_geohash"
    success_keys: ClassVar[Tuple[str, ...]] = (
        "column_A",
        "column_B",
        "ignore_row_if",
        "mostly",
    )

    # This dictionary contains metadata for display in the public gallery
    library_metadata: ClassVar[dict] = {
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


if __name__ == "__main__":
    ExpectColumnPairValuesLatLngMatchesGeohash().print_diagnostic_checklist()
