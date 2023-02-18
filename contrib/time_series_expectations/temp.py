"""

This module contains 4 Data Assistants:

    * Freshness
    * Volume
    * RowLevelTimeSeries
    * MultiBatchTimeSeries / BatchLevelTimeSeries

* Freshness : "I always want to know how fresh my data is"
* Volume: "I want to know if data is arriving at the expected rate"
* RowLevelTimeSeries: "I have numeric time series data in a table, file, etc. I want to learn what patterns are normal in that data, and then monitor it for anomalies"
* BatchLevelTimeSeries: "I have a a data asset with multiple batches arriving over time. I want to learn what patterns are normal in that data, and then monitor it for anomalies"




This module introduces several new Expectations, and abstract base classes.

The class hiereachy is as follows.

    *TableExpectation* (ABC)
        BatchAggregateStatisticExpectation (ABC)
            ExpectBatchAggregateStatisticToBeBetween (ABC)
                expect_batch_update_time_to_be_between
                expect_batch_volume_to_be_between

            ExpectBatchAggregateStatisticToMatchProphetDateModel (ABC)
                expect_batch_volume_to_match_prophet_date_model

            ExpectBatchAggregateStatisticToMatchProphetTimestampModel (ABC)
                expect_batch_volume_to_match_prophet_timestamp_model

            ExpectBatchAggregateStatisticToMatchArimaModel (ABC)
                expect_batch_volume_to_match_arima_model

        *ColumnPairMapExpectation* (ABC)
            ColumnPairTimeSeriesExpectation (ABC)
                expect_column_pair_values_to_match_prophet_date_model
                expect_column_pair_values_to_match_prophet_timestamp_model
                expect_column_pair_values_to_match_arima_model

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

        
Formatting conventions:

* Abstract base classes are in camel case, and marked with (ABC).
* Concrete classes are in snake case.
* Classes from the core Expectations library are in *italics*.






This module introduces several new Expectations, inheriting from some new abstract base classes. The most important ABCs are

* BatchAggregateStatisticTimeSeriesExpectation: Expectations inheriting from this class take a Batch of data, compute a single statistic (e.g. the size, mean, or sum) from that batch, and compare it to a pre-trained time series model. This is useful for computing things like volume and freshness for data that arrives in Batches over time. BatchAggregateStatisticTimeSeriesExpectations share the following parameters:
    * date or timestamp: The date/time stamp to use for the model. If None, the current date/time is used.
    * date_format or timestamp_format: The format of the date. If None, the date is assumed to be a datetime object.
    * zscores: The number of standard deviations to use for the upper and lower bounds.
    * model: A time series model, such as Prophet or ARIMA.
    * Any other parameters that are required to compute the aggregate statistic.

* ColumnAggregateTimeSeriesExpectation: Expectations that take a Batch of data with numeric data  compute a statistic from each row, and compare it to a time series model. Expectations inheriting from this class share the following parameters:

* ColumnPairTimeSeriesExpectation: Expectations inheriting from this class operate within a single Batch. This Batch must have a column of numeric data and a column of date/time stamps. The expectation compares numeric values from each row with the predicted values from a pre-trained time series model. This is a subclass of ColumnPairMapExpectation. ColumnPairTimeSeriesExpectations share the following parameters:
    * model: A time series model, such as Prophet or ARIMA.
    * date_column or timestamp_column: The column containing the date/time stamp to use for the model.
    * value_column: The column containing the value to use for the model.
    * zscores: The number of standard deviations to use for the upper and lower bounds.


"""