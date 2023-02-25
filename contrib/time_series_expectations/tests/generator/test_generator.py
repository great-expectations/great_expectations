from time_series_expectations.generator.daily_time_series_generator import (
    DailyTimeSeriesGenerator,
)

def test__imports():
    from time_series_expectations.generator import DailyTimeSeriesGenerator

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
        trend_params=[{
            "alpha": 0.0,
            "beta": 0.1,
            "cutpoint": 100,
        },{
            "alpha": 10.0,
            "beta": -0.1,
            "cutpoint": 200,
        }],
        weekday_dummy_params=[5, 6, 7, 6, 5, 1, 0],
        annual_seasonality_params=[
            (1.3 , -2.4),
            (1.3 , -0.3),
            (-0.5,  0.3),
            (-1.2,  2.1),
            (-2.0,  -0.2),
            (-2.1,  1.0),
            (-1.2,  -1.3),
            (-1.6,  1.0),
            (-1.3,  0.8),
            (-0.0,  0.7),
        ],
        holiday_alpha=3.5,
        outlier_alpha=2.5,
        noise_scale=1.0,
    )