from time_series_expectations.generator import (
    generate_time_series,
    generate_time_series_df,
)


def test_smoke__generate_time_series_df():
    generate_time_series_df()


def test_smoke__generate_time_series():
    generate_time_series()
