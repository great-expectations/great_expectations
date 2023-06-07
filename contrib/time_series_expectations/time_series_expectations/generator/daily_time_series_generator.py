from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from time_series_expectations.generator.time_series_generator import (
    TimeSeriesGenerator,
    TrendParams,
)


class DailyTimeSeriesGenerator(TimeSeriesGenerator):
    """Generate a daily time series with trend, seasonality, and outliers."""

    def _generate_trend(
        self,
        date_range: np.ndarray,
        trend_params: List[TrendParams],
    ) -> np.ndarray:
        """Generate a trend component for a time series."""

        X = date_range * 0
        prev_cutpoint = 0
        for param_set in trend_params:
            X[prev_cutpoint : param_set["cutpoint"]] = param_set["alpha"] + param_set[
                "beta"
            ] * (date_range[prev_cutpoint : param_set["cutpoint"]] - prev_cutpoint)
            prev_cutpoint = param_set["cutpoint"]
        return X

    def _generate_weekday_seasonality(
        self,
        date_range: np.ndarray,
        weekday_dummy_params: List[float],
    ) -> np.ndarray:
        """Generate a weekday seasonality component for a time series."""

        return np.array([weekday_dummy_params[t % 7] for t in date_range])

    def _generate_annual_seasonality(
        self,
        date_range: np.ndarray,
        annual_seasonality_params: List[Tuple[float, float]],
    ) -> np.ndarray:
        """Generate an annual seasonality component for a time series."""

        return sum(
            [
                alpha * np.cos(2 * np.pi * (i + 1) * date_range / 365)
                + beta * np.sin(2 * np.pi * (i + 1) * date_range / 365)
                for i, (alpha, beta) in enumerate(annual_seasonality_params)
            ]
        )

    def _generate_posneg_pareto(
        self,
        alpha: float,
        size: int,
    ):
        """Generate a positive or negative pareto distribution."""

        if alpha is not None:
            return np.random.pareto(a=alpha, size=size) * np.random.randint(
                -1, 2, size=size
            )
        else:
            return 0

    def _generate_component_time_series(
        self,
        size: int,
        trend_params: List[TrendParams],
        weekday_dummy_params: List[float],
        annual_seasonality_params: List[Tuple[float, float]],
        holiday_alpha: float,
        outlier_alpha: float,
        noise_scale: float,
    ):
        """Generate the components of a time series."""

        date_range = np.arange(size)

        trend = self._generate_trend(date_range, trend_params)
        weekly_seasonality = self._generate_weekday_seasonality(
            date_range, weekday_dummy_params
        )
        annual_seasonality = self._generate_annual_seasonality(
            date_range, annual_seasonality_params
        )
        holidays = self._generate_posneg_pareto(holiday_alpha, size)
        outliers = self._generate_posneg_pareto(outlier_alpha, size)
        noise = np.random.normal(scale=noise_scale, size=size)

        return {
            "trend": trend,
            "weekly_seasonality": weekly_seasonality,
            "annual_seasonality": annual_seasonality,
            "holidays": holidays,
            "outliers": outliers,
            "noise": noise,
        }

    def _generate_daily_time_series(
        self,
        size: int = 365 * 3,
        trend_params: Optional[List[TrendParams]] = None,
        weekday_dummy_params: Optional[List[float]] = None,
        annual_seasonality_params: Optional[List[Tuple[float, float]]] = None,
        holiday_alpha: float = 3.5,
        outlier_alpha: float = 2.5,
        noise_scale: float = 1.0,
    ) -> np.ndarray:
        """Generate a time series."""

        if trend_params is None:
            # Generate a time series with 4 trend segments
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
            # Create 7 random weekday dummies
            weekday_dummy_params = [np.random.normal() for i in range(7)]

        if annual_seasonality_params is None:
            # Create 10 random annual seasonality parameters
            annual_seasonality_params = [
                (
                    np.random.normal(),
                    np.random.normal(),
                )
                for i in range(10)
            ]

        time_series_components = self._generate_component_time_series(
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

    def generate_df(
        self,
        size: Optional[int] = 365 * 3,
        start_date: Optional[str] = "2018-01-01",
        trend_params: Optional[List[TrendParams]] = None,
        weekday_dummy_params: Optional[List[float]] = None,
        annual_seasonality_params: Optional[List[Tuple[float, float]]] = None,
        holiday_alpha: float = 3.5,
        outlier_alpha: float = 2.5,
        noise_scale: float = 1.0,
    ) -> pd.DataFrame:
        """Generate a time series as a pandas dataframe.

        Keyword Args:
            size: The number of days in the time series.
            start_date: The start date of the time series.
            trend_params: A list of trend parameters corresponding to cutpoints in the time series.
            weekday_dummy_params: A list of weekday dummy parameters. Should be a list of length 7, with each day corresponding to the average difference in the time series on that day.
            annual_seasonality_params: A list of annual seasonality parameters used to create a cyclic component in the time series.
            holiday_alpha: The alpha parameter for the pareto distribution used to generate holiday effects.
            outlier_alpha: The alpha parameter for the pareto distribution used to generate outlier effects.
            noise_scale: The scale parameter for the standard deviation of the normal distribution used to generate noise.

        Returns:
            A pandas dataframe with a date column and a time series column.

        Notes:
            * Holiday and outlier effects are generated using a pareto distribution. The alpha parameter controls the shape of the distribution. A higher alpha value will result in more extreme holiday and outlier effects.
            * Holidays don't correspond to actual holidays. Instead, they are generated by randomly selecting days in the time series.
            * Annual seasonality is generated by Fourier series. The number of fourier terms is determined by the length of the annual_seasonality_params list. The first element of each tuple in the list is the amplitude of the sine term, and the second element is the amplitude of the cosine term.
        """

        return pd.DataFrame(
            {
                "ds": pd.date_range(start_date, periods=size, freq="D"),
                "y": self._generate_daily_time_series(
                    size,
                    trend_params,
                    weekday_dummy_params,
                    annual_seasonality_params,
                    holiday_alpha,
                    outlier_alpha,
                    noise_scale,
                ),
            }
        )
