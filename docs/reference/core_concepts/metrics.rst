.. _metrics:

##############
Metrics
##############

.. warning::

    Metrics are **experimental** in Great Expectations. Expect changes to the API.


Metrics are values derived from one or more Data Assets that can be used to evaluate expectations or to summarize the result of Validation. A metric could be a statistic, such as the minimum value of the column, or a more complex object, such as a histogram.


Expectation Validation Results and Expectation Suite Validation Results can expose metrics that are defined by specific expectations that have been validated, called "Expectation Defined Metrics." In the future, we plan to allow Expectations to have more control over the metrics that they generate, expose, and use in testing. A Metric is obtained from an ExpectationValidationResult or ExpectationSuiteValidationResult by providing the `metric_name` and `metric_kwargs` or `metric_kwargs_id`.

A metric name is a dot-delimited string that identifies the value, such as `expect_column_values_to_be_unique .success` or `expect_column_values_to_be_between.result.unexpected_percent`.

Metric Kwargs are key-value pairs that identify the metric within the context of the validation, such as "column": "Age". Different metrics may require different Kwargs.

A ``metric_kwargs_id`` is a string representation of the Metric Kwargs that can be used as a database key. For simple cases, it could be easily readable, such as `column=Age`, but when there are multiple keys and values or complex values, it will most likely be an md5 hash of key/value pairs. It can also be None in the case that there are no kwargs required to identify the metric.

The following examples demonstrate how metrics are defined:

.. code-block:: python

    res = df.expect_column_values_to_be_in_set("Sex", ["male", "female"])
    res.get_metric("expect_column_values_to_be_in_set.result.missing_count", column="Sex")


See :ref:`How to configure a MetricsStore <saving_metrics>` guide for more information.
