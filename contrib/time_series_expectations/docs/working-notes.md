### TimeSeriesExpectations
Expectations for detecting trends, seasonality, outliers, etc. in time series data

Author: Abe Gong ([abegong](https://github.com/abegong))

[PyPi Link](https://pypi/python.org/pypi/time_series_expectations)

### WIP Warning

This is a rough draft of a future README for this package. Think of it as the contributor version of a design doc.

```
WIP WIP WIP WIP WIP WIP WIP WIP WIP WIP WIP WIP WIP WIP WIP WIP WIP WIP
WIP                                                                 WIP
WIP WARNING: this README is aspirational. Do not believe it yet.    WIP
WIP                                                                 WIP
WIP WIP WIP WIP WIP WIP WIP WIP WIP WIP WIP WIP WIP WIP WIP WIP WIP WIP
```

### Overview
This package contains...

* Four Data Assistants for creating time series Expectations: Freshness, Volume, BatchLevelTimeSeries, and RowLevelTimeSeries.
* Expectations for detecting trends, seasonality, outliers, etc. in time series data
* Abstract Base Classses for creating additional time series Expectations.
* Methods for generating time series data for testing purposes.
* Methods for pulling illustrative data from real-world sources.


### Installation

`time_series_expectations` has optional dependencies on several time series packages:

* [Prophet]({link})
* [`statsmodels`]({link})
* [`NeuralProphet`]({link})
* [`pmdarima`]({link})

Each of these package has its own dependencies, strengths, and weaknesses. Please visit your favorite search engine and/or these external links for more information.

* ...
* https://www.machinelearningplus.com/time-series/arima-model-time-series-forecasting-python/
* https://neptune.ai/blog/arima-vs-prophet-vs-lstm


You can install any of those packages like this: `pip install time_series_expectations[prophet, statsmodels]`

Technically, `pip install time_series_expectations` is also allowed. But if you don't specify a time series dependency at all, very few of the Expectations in this package will work.


### Data Assistants and Usage

This module contains four new DataAssistants, which serve as the primary entry points for the package.

Warning: These docs assume that you have completed the Great Expectations onboarding tutorial, and are familiar with basic concepts including [Expectations](link), [DataAssistants](link), and [DataSources](link).

* Freshness : "I always want to know how fresh my data is"

```
freshness_expectation_suite = context.assistants.freshness.profile_data_asset(
    context.sources.my_db.assets.my_table
)
```

* Volume: "I want to know if data is arriving at the expected rate"

```
volume_expectation_suite = context.assistants.volume.profile_data_asset(
    context.sources.my_db.assets.my_table
)
```

* BatchLevelTimeSeries: "I have a data asset with multiple batches arriving over time. I want to learn what patterns are normal in that data, and then monitor it for anomalies"

```
time_series_expectation_suite = context.assistants.time_series_by_batch.profile_data_asset(
    context.sources.my_db.assets.my_table,
    columns=["foo", "bar", "baz"],
    metrics=["mean", "median", "sum", "percent_null"]
)

time_series_expectation_suite = context.assistants.time_series_by_batch.profile_data_asset(
    context.sources.my_db.assets.my_table,
    columns=["foo", "bar", "baz"],
    metric="percent_matching_regex",
    kwargs={
        "regex": "\d+",
    }
)
```

* RowLevelTimeSeries: "I have numeric time series data in a table, file, etc. I want to learn what patterns are normal in that data, and then monitor it for anomalies"

time_series_expectation_suite = context.assistants.time_series_by_row.profile_data_asset(
    context.sources.my_db.assets.my_table,
    date_column="created_at",
    columns=["foo", "bar", "baz"],
    metrics=["mean", "median", "sum", "percent_null"]
)

### New Expectations

This module introduces several new Expectations, inheriting from some new abstract base classes.

You can learn more about the Expectations in the Expectation gallery, [here](link).


### Abstract Base Classes

The most important ABCs are [BatchAggregateStatisticTimeSeriesExpectation](link), [ColumnAggregateTimeSeriesExpectation](link), and  [ColumnPairTimeSeriesExpectation](link). They allow time series models to be applied to data in a variety of shapes and formats. Please see the class docstrings for more detailed explanation.

The full class hierarchy is:

    *BatchExpectation* (ABC)
        BatchAggregateStatisticExpectation (ABC)
            ExpectBatchAggregateStatisticToBeBetween (ABC)
                expect_batch_update_time_to_be_between
                expect_batch_volume_to_be_between

            BatchAggregateStatisticTimeSeriesExpectation (ABC)
                ExpectBatchAggregateStatisticToMatchProphetDateModel (ABC)
                    expect_batch_volume_to_match_prophet_date_model

                ExpectBatchAggregateStatisticToMatchProphetTimestampModel (ABC)
                    expect_batch_volume_to_match_prophet_timestamp_model

                ExpectBatchAggregateStatisticToMatchArimaModel (ABC)
                    expect_batch_volume_to_match_arima_model

        *ColumnAggregateExpectation* (ABC)
            ColumnAggregateTimeSeriesExpectation (ABC)
                expect_column_{property}_to_match_{model}_model

                    properties:
                        max
                        min
                        stdev
                        sum
                        percent_missingness
                        percent_matching_regex
                        ...

                    models:
                        prophet_date
                        prophet_timestamp
                        arima

        *ColumnPairMapExpectation* (ABC)
            ColumnPairTimeSeriesExpectation (ABC)
                expect_column_pair_values_to_match_prophet_date_model
                expect_column_pair_values_to_match_prophet_timestamp_model
                expect_column_pair_values_to_match_arima_model
        
Formatting conventions:

* Abstract base classes are in camel case, and marked with (ABC).
* Concrete classes are in snake case.
* Classes from the core Expectations library are in *italics*.

### Methods for generating synthetic time series data

```
generate_time_series_df()
```

See [the script that creates examples](link) and [the API docs](link) for additional examples

### Real-world data sources

Things like these datasets would be nifty:

    * pypi downloads
    * github stars
    * stock market data
    * https://steamcharts.com/

### anonymous metrics

Related to "Add SQL implementation for `expect_column_pair_values_to_match_prophet_date_model` and other row-level metrics."

https://github.com/great-expectations/great_expectations/pull/3485/files