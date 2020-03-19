.. _metrics:

##############
Metrics
##############

Metrics are values derived from one or more Data Assets that can be used to evaluate expectations or to summarize the
result of Validation. A Metric is obtained from an ExpectationValidationResult or ExpectationSuiteValidationResult by
providing the `metric_name` and `metric_kwargs` or `metric_kwargs_id`.

A metric name is a dot-delimited string that identifies the value, such as `expect_column_values_to_be_unique
.success` or `expect_column_values_to_be_between.result.unexpected_percent`.

Metric Kwargs are key-value pairs that identify the metric within the context of the validation, such as "column":
"Age". Different metrics may require different Kwargs.

A metric_kwargs_id is a string representation of the Metric Kwargs that can be used as a database key. For simple
cases, it could be easily readable, such as `column=Age`, but when there are multiple keys and values or complex
values, it will most likely be an md5 hash of key/value pairs. It can also be None in the case that there are no
kwargs required to identify the metric.

See the :ref:`metrics_reference` or :ref:`Saving Metrics Tutorial <saving_metrics>` for more information.

*Last updated:* |lastupdate|
