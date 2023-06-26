from io import StringIO

import numpy as np
import pandas as pd


def generate_trend(time, trend_params) -> np.ndarray:
    """Generate a trend component for a time series."""

    X = time * 0
    prev_cutpoint = 0
    for param_set in trend_params:
        X[prev_cutpoint : param_set["cutpoint"]] = param_set["alpha"] + param_set[
            "beta"
        ] * (time[prev_cutpoint : param_set["cutpoint"]] - prev_cutpoint)
        prev_cutpoint = param_set["cutpoint"]
    return X


def generate_weekday_seasonality(
    time,
    weekday_dummy_params,
) -> np.ndarray:
    """Generate a weekday seasonality component for a time series."""

    return np.array([weekday_dummy_params[t % 7] for t in time])


def generate_annual_seasonality(
    time,
    annual_seasonality_params,
) -> np.ndarray:
    """Generate an annual seasonality component for a time series."""

    return sum(
        [
            alpha * np.cos(2 * np.pi * (i + 1) * time / 365)
            + beta * np.sin(2 * np.pi * (i + 1) * time / 365)
            for i, (alpha, beta) in enumerate(annual_seasonality_params)
        ]
    )


def generate_posneg_pareto(alpha, size):
    """Generate a positive or negative pareto distribution."""

    if alpha is not None:
        return np.random.pareto(a=alpha, size=size) * np.random.randint(
            -1, 2, size=size
        )
    else:
        return 0


def generate_component_time_series(
    size,
    trend_params,
    weekday_dummy_params,
    annual_seasonality_params,
    holiday_alpha,
    outlier_alpha,
    noise_scale,
):
    """Generate the components of a time series."""

    time = np.arange(size)

    trend = generate_trend(time, trend_params)
    weekly_seasonality = generate_weekday_seasonality(time, weekday_dummy_params)
    annual_seasonality = generate_annual_seasonality(time, annual_seasonality_params)
    holidays = generate_posneg_pareto(holiday_alpha, size)
    outliers = generate_posneg_pareto(outlier_alpha, size)
    noise = np.random.normal(scale=noise_scale, size=size)

    return {
        "trend": trend,
        "weekly_seasonality": weekly_seasonality,
        "annual_seasonality": annual_seasonality,
        "holidays": holidays,
        "outliers": outliers,
        "noise": noise,
    }


def generate_daily_time_series(
    size=365 * 3,
    trend_params=None,
    weekday_dummy_params=None,
    annual_seasonality_params=None,
    holiday_alpha=3.5,
    outlier_alpha=2.5,
    noise_scale=1.0,
) -> np.ndarray:
    """Generate a time series."""

    if trend_params is None:
        trend_params = [
            {
                "alpha": 0,
                "beta": 0.06,
                "cutpoint": int(size / 4),
            },
            {
                "alpha": 25,
                "beta": -0.05,
                "cutpoint": int(size / 2),
            },
            {
                "alpha": 5,
                "beta": 0.0,
                "cutpoint": int(3 * size / 4),
            },
            {
                "alpha": 0,
                "beta": 0.08,
                "cutpoint": size,
            },
        ]

    if weekday_dummy_params is None:
        weekday_dummy_params = [np.random.normal() for i in range(7)]

    if annual_seasonality_params is None:
        annual_seasonality_params = [
            (
                np.random.normal(),
                np.random.normal(),
            )
            for i in range(10)
        ]

    time_series_components = generate_component_time_series(
        size,
        trend_params,
        weekday_dummy_params,
        annual_seasonality_params,
        holiday_alpha,
        outlier_alpha,
        noise_scale,
    )

    Y = (
        time_series_components["trend"]
        + time_series_components["weekly_seasonality"]
        + time_series_components["annual_seasonality"]
        + time_series_components["holidays"]
        + time_series_components["outliers"]
        + time_series_components["noise"]
    )

    return Y


def generate_daily_time_series_df(
    size: int = 365 * 3, start_date: str = "2018-01-01", **kwargs
) -> pd.DataFrame:
    """Generate a time series as a pandas dataframe."""

    return pd.DataFrame(
        {
            "ds": pd.date_range(start_date, periods=size, freq="D"),
            "y": generate_daily_time_series(size, **kwargs),
        }
    )


if __name__ == "__main__":
    output = StringIO()

    df = generate_daily_time_series_df()
    df.to_csv(output, index=False)

    print(output.getvalue())
