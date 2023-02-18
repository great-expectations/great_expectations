### TimeSeriesExpectations
Expectations for detecting trends, seasonality, outliers, etc. in time series data

Author: Abe Gong ([abegong](https://github.com/abegong))

[PyPi Link](https://pypi/python.org/pypi/time_series_expectations)


### Overview
This module contains...

* Expectations for detecting trends, seasonality, outliers, etc. in time series data
* Four Data Assistants for creating time series Expectations: Freshness, Volume, BatchLevelTimeSeries, and RowLevelTimeSeries.
* Methods for generating time series data for testing purposes.
* Methods for pulling illustrative data from real-world sources.
    * pypi downloads
    * github stars
    * stock market data
    * https://steamcharts.com/

### Installation

`pip install time_series_expectations`



`pip install time_series_expectations[prophet]`

`pip install time_series_expectations[arima]`

### Data Assistants and Usage

This module contains four new Data Assistants, which serve as the primary entry points for most usage.

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

* BatchLevelTimeSeries: "I have a a data asset with multiple batches arriving over time. I want to learn what patterns are normal in that data, and then monitor it for anomalies"

```
time_series_expectation_suite = context.assistants.volume.profile_data_asset(
    context.sources.my_db.assets.my_table,
    columns=["foo", "bar", "baz"],
    metrics=["mean", "median", "sum", "percent_null"]
)

time_series_expectation_suite = context.assistants.volume.profile_data_asset(
    context.sources.my_db.assets.my_table,
    columns=["foo", "bar", "baz"],
    metric="percent_matching_regex",
    kwargs={
        "regex": "\d+",
    }
)
```


* RowLevelTimeSeries: "I have numeric time series data in a table, file, etc. I want to learn what patterns are normal in that data, and then monitor it for anomalies"

### Expectations and Abstract Base Classes

This module introduces several new Expectations, inheriting from some new abstract base classes.

You can learn more about the Expectations in the Expectation gallery, [here](link).

The most important ABCs are BatchAggregateStatisticTimeSeriesExpectation, ColumnPairTimeSeriesExpectation, and ColumnAggregateTimeSeriesExpectation. They allow time series models to be applied to data in a variety of shapes and formats. Please see the class docstrings for more detailed explanation.



as follows. You can find more details about 

The full class hiereachy is:

    *TableExpectation* (ABC)
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

        *ColumnExpectation* (ABC)
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
