from time_series_expectations.generator.daily_time_series_generator import (
    DailyTimeSeriesGenerator,
)
from time_series_expectations.generator.hourly_time_series_generator import (
    HourlyTimeSeriesGenerator,
)
from time_series_expectations.generator.monthly_time_series_generator import (
    MonthlyTimeSeriesGenerator,
)
from time_series_expectations.generator.weekly_time_series_generator import (
    WeeklyTimeSeriesGenerator,
)


def test__imports():
    pass


def test__generate_daily_time_series_df():
    generator = DailyTimeSeriesGenerator()

    df = generator.generate_df()
    assert df.shape == (365 * 3, 2)

    df = generator.generate_df(
        size=20,
    )
    assert df.shape == (20, 2)

    df = generator.generate_df(
        size=200,
        trend_params=[
            {
                "alpha": 0.0,
                "beta": 0.1,
                "cutpoint": 100,
            },
            {
                "alpha": 10.0,
                "beta": -0.1,
                "cutpoint": 200,
            },
        ],
        weekday_dummy_params=[5, 6, 7, 6, 5, 1, 0],
        annual_seasonality_params=[
            (1.3, -2.4),
            (1.3, -0.3),
            (-0.5, 0.3),
            (-1.2, 2.1),
            (-2.0, -0.2),
            (-2.1, 1.0),
            (-1.2, -1.3),
            (-1.6, 1.0),
            (-1.3, 0.8),
            (-0.0, 0.7),
        ],
        holiday_alpha=3.5,
        outlier_alpha=2.5,
        noise_scale=1.0,
    )


def test__generate_weekly_time_series_df():
    generator = WeeklyTimeSeriesGenerator()

    df = generator.generate_df()
    assert df.shape == (52 * 3, 2)


def test__generate_monthly_time_series_df():
    generator = MonthlyTimeSeriesGenerator()

    df = generator.generate_df(
        size=20,
    )
    assert df.shape == (20, 2)


def test__generate_hourly_time_series_df():
    generator = HourlyTimeSeriesGenerator()

    df = generator.generate_df()
    assert df.shape == (90 * 24, 2)

    df = generator.generate_df(hourly_seasonality_params=[(1.0, 0.0)])
    assert df.shape == (90 * 24, 2)

    # Test ability to generate data that's NOT in 24 hour increments
    df = generator.generate_df(
        size=25,
    )
    assert df.shape == (25, 2)
