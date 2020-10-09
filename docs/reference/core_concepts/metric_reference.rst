.. _metrics_reference:


#######################
Metrics Reference
#######################

Metrics are still a **beta feature** in Great Expectations. Expect changes to the API.

A Metric is a value that Great Expectations can use to evaluate expectations or to store externally. A metric could
be a statistic, such as the minimum value of the column, or a more complex object, such as a histogram.

Expectation Validation Results and Expectation Suite Validation Results can expose metrics that are defined by
specific expectations that have been validated, called "Expectation Defined Metrics." In the future, we plan to allow
Expectations to have more control over the metrics that they generate, expose, and use in testing.

The following examples demonstrate how metrics are defined:

.. code-block:: python

    res = df.expect_column_values_to_be_in_set("Sex", ["male", "female"])
    res.get_metric("expect_column_values_to_be_in_set.result.missing_count", column="Sex")
