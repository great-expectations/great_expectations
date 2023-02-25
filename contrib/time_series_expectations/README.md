### TimeSeriesExpectations
Expectations for detecting trends, seasonality, outliers, etc. in time series data

Author: Abe Gong ([abegong](https://github.com/abegong))

[PyPi Link](https://pypi/python.org/pypi/time_series_expectations)

### Overview
This package currently contains...

* Expectations for detecting trends, seasonality, outliers, etc. in time series data
* An abstract base class for creating additional time series Expectations based on column aggregate metrics.
* Methods for generating time series data for testing purposes.

### Expectations

This package currently contains 3 new Expectations. These are examples of the primary patterns that future time series Expectations will follow.

    expect_batch_row_count_to_match_prophet_date_model
    expect_column_max_to_match_prophet_date_model
    expect_column_pair_values_to_match_prophet_date_model

`expect_batch_row_count_to_match_prophet_date_model` and `expect_column_max_to_match_prophet_date_model` are Batch-level Expectations: each Batch corresponds to a single timestamp-value pair in a time series. In typical usage, you would expect to validate a single Batch (and therefore a single timestamp-value) at a time. `expect_column_pair_values_to_match_prophet_date_model` is a row-level Expectation: a Batch will typically contain many timestamp-values, which can be evaluated together.


Backend support:
* All three Expectations work in pandas
* Both batch-level Expectations work in SQL. The supporting metric for `expect_column_pair_values_to_match_prophet_date_model` is not yet implemented in SQL.
* All three Expectations work in Spark, but `expect_column_pair_values_to_match_prophet_date_model` relies on a UDF that may be slow for large data sets.

### Methods for generating synthetic time series data

```
generate_time_series_df()
```

See [the script that creates examples](link) and [the API docs](link) for additional examples

### Future work

* Add better documentation and better testing for synthetic time series generation
* Add support for additional time series grains (not just daily)
* Add SQL implementation for `expect_column_pair_values_to_match_prophet_date_model` and other row-level metrics.
* Add Data Assistants for creating time series Expectations
* Add better renderers for time series Expectations, including graphs produced by `altair`
* Add methods for pulling illustrative time series data from real-world sources
* Add more options for time series models, other than `prophet` (e.g. `statsmodels.tst.arima`, `pdarima`, `NeuralProphet`)



### Design notes on future class hierarchy

As all of those use cases are realized, we imagine the full class hiereachy for time series Expectations to evolve into this:

    *TableExpectation* (ABC)
        *BatchAggregateStatisticExpectation* (ABC)
            BatchAggregateStatisticTimeSeriesExpectation (ABC)
                ExpectBatchAggregateStatisticToMatchProphetDateModel (ABC)
                    expect_batch_row_count_to_match_prophet_date_model (:white_check_mark:)
                    expect_batch_most_recent_update_to_match_prophet_date_model

                ExpectBatchAggregateStatisticToMatchProphetTimestampModel (ABC)
                    expect_batch_row_count_to_match_prophet_timestamp_model
                    expect_batch_most_recent_update_to_match_prophet_timestamp_model

                ExpectBatchAggregateStatisticToMatchArimaModel (ABC)
                    expect_batch_row_count_to_match_arima_model
                    expect_batch_most_recent_update_to_match_arima_model
                
                ... for other types of models

        *ColumnExpectation* (ABC)
            ColumnAggregateTimeSeriesExpectation (ABC, :white_check_mark:)
                expect_column_max_to_match_prophet_date_model (:white_check_mark:)
                expect_column_{property}_to_match_{model}_model
                ...

        *ColumnPairMapExpectation* (ABC, :white_check_mark:)
            ColumnPairTimeSeriesExpectation (ABC)
                expect_column_pair_values_to_match_prophet_date_model (:white_check_mark:)
                expect_column_pair_values_to_match_prophet_timestamp_model
                expect_column_pair_values_to_match_arima_model
        
Formatting conventions for the nested hierarchy above:

* Abstract base classes are in CamelCase, and marked with (ABC).
* Concrete classes are in snake_case.
* Classes that live/will live in the core Great Expectations library are in *italics*.
* Classes that have already been implemented are marked with :white_check_mark:

About Abstract Base Classes:

The most important ABCs are `BatchAggregateStatisticTimeSeriesExpectation`, [ColumnAggregateTimeSeriesExpectation](link), and  `ColumnPairTimeSeriesExpectation`. They allow time series models to be applied to data in a variety of shapes and formats. Like most ABCs, these classes won't be executable themselves, but will hold shared logic to make it easier to create and maintain Expectations that follow certain patterns.


About `ColumnAggregateTimeSeriesExpectations`:

We expect these to be an `n` by `k` matrix of `n` metrics and `k` models. Once we've fully established the right patterns for these Expectations (and their Renderers and Profilers), we should be able to code-gen most or all of these Expectations, to quickly expand coverage.

    metrics:
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
        ...
