from time_series_expectations.generator import (
    generate_time_series,
    generate_time_series_df,
)


def test_smoke__generate_time_series_df():
    generate_time_series_df()


def test__generate_time_series_df():
    df = generate_time_series_df()
    assert df.shape == (365 * 3, 2)

    df = generate_time_series_df(
        size=20,
    )
    assert df.shape == (20, 2)


def test_smoke__generate_time_series():
    generate_time_series()
