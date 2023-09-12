"""Generate test data for time series. Includes both CSVs and PNGs"""

import os.path

import matplotlib.pyplot as plt
import numpy as np

from great_expectations.data_context.util import file_relative_path
from time_series_expectations.generator import (
    DailyTimeSeriesGenerator,
    HourlyTimeSeriesGenerator,
)

plt.rcParams["figure.figsize"] = (20, 3)


def generate_time_series_data_and_plot(
    grain: str,
    size: int,
    intercept: float,
    # noise,
    trend: float,
    hourly_seasonality: float,
    weekly_seasonality: float,
    # annual_seasonality,
    outlier_alpha: float,
):
    filename = f"{grain}__size_{size}__trend_{trend}__weekly_seasonality_{weekly_seasonality}__outliers_{outlier_alpha}"
    print(filename)

    if grain == "daily":
        generator = DailyTimeSeriesGenerator()
        df = generator.generate_df(
            size=size,
            trend_params=[
                {
                    "alpha": intercept,
                    "beta": trend,
                    "cutpoint": size,
                }
            ],
            weekday_dummy_params=[
                weekly_seasonality * x for x in [0, 0.5, 3.5, 4, 5, -3, -3.5]
            ],
            annual_seasonality_params=[],
            holiday_alpha=1000,
            outlier_alpha=outlier_alpha,
        )

    elif grain == "hourly":
        generator = HourlyTimeSeriesGenerator()
        df = generator.generate_df(
            size=size,
            trend_params=[
                {
                    "alpha": intercept,
                    "beta": trend,
                    "cutpoint": size,
                }
            ],
            hourly_seasonality=hourly_seasonality,
            weekday_dummy_params=[
                weekly_seasonality * x for x in [0, 0.5, 3.5, 4, 5, -3, -3.5]
            ],
            annual_seasonality_params=[],
            holiday_alpha=1000,
            outlier_alpha=outlier_alpha,
        )

    else:
        raise ValueError(f"Invalid grain: {grain}")

    df.to_csv(
        os.path.join(  # noqa: PTH118
            file_relative_path(__file__, "data"), f"{filename}.csv"
        ),
        index=None,
    )

    plt.plot(df.y)
    plt.savefig(
        os.path.join(  # noqa: PTH118
            file_relative_path(__file__, "pics"), f"{filename}.png"
        ),
    )
    plt.clf()


# Generate a bunch of test data: daily, 180 days long, with intercept 100
# These are good for showing the effect of different parameters

np.random.seed(7)

generate_time_series_data_and_plot(
    grain="daily",
    size=180,
    intercept=100,
    trend=0,
    hourly_seasonality=0,
    weekly_seasonality=0,
    outlier_alpha=1000,
)

generate_time_series_data_and_plot(
    grain="daily",
    size=180,
    intercept=100,
    trend=0.05,
    hourly_seasonality=0,
    weekly_seasonality=0,
    outlier_alpha=1000,
)

generate_time_series_data_and_plot(
    grain="daily",
    size=180,
    intercept=100,
    trend=0.05,
    hourly_seasonality=0,
    weekly_seasonality=1,
    outlier_alpha=1000,
)

np.random.seed(7)

generate_time_series_data_and_plot(
    grain="daily",
    size=180,
    intercept=100,
    trend=0.05,
    hourly_seasonality=0,
    weekly_seasonality=1,
    outlier_alpha=2,
)


generate_time_series_data_and_plot(
    grain="hourly",
    size=24 * 180,
    intercept=100,
    trend=0.05,
    hourly_seasonality=3,
    weekly_seasonality=1,
    outlier_alpha=1000,
)

generate_time_series_data_and_plot(
    grain="hourly",
    size=24 * 180,
    intercept=100,
    trend=0.05,
    hourly_seasonality=3,
    weekly_seasonality=1,
    outlier_alpha=4,
)

generate_time_series_data_and_plot(
    grain="hourly",
    size=24 * 180,
    intercept=100,
    trend=0.05,
    hourly_seasonality=3,
    weekly_seasonality=1,
    outlier_alpha=3,
)

generate_time_series_data_and_plot(
    grain="hourly",
    size=24 * 180,
    intercept=100,
    trend=0.05,
    hourly_seasonality=3,
    weekly_seasonality=1,
    outlier_alpha=2,
)
