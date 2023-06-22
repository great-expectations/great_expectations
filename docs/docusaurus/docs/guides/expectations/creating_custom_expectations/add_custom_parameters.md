---
sidebar_label: 'Add custom parameters to Custom Expectations'
title: 'Add custom parameters to Custom Expectations'
id: add_custom_parameters
description: Add custom parameters to Custom Expectations.
---

Use custom parameters in your Custom Expectations when you have data dependencies that can’t or shouldn’t be hardcoded. The Custom Expectation class determines how you implement custom parameters.

Using custom parameters in your Custom Expectations can help you create powerful, business-specific validations and help you optimize your Great Expectations (GX) workflows.

## Implementation

All Custom Expectations include a `success_keys` attribute that is a tuple of strings. The tuple items are the names of the parameters which are available during the evaluation of your Custom Expectation. For example, the `success_keys` attribute for the ColumnPairMapExpectation appears similar to the following example:

```python
success_keys = (
   "column_A",
   "column_B",
   "mostly",
)
```
The tuple in the example includes the two columns being evaluated and the `mostly` parameter. These parameters are passed as a part of your ExpectationConfiguration and are available to the Expectation `_validate` method.

## Use case

In this use case, a `condition_value_keys` tuple and a `condition_domain_keys` tuple are added to ColumnPairMapMetrics, MulticolumnMapMetrics, and ColumnMapMetrics. The `condition_value_keys` tuple supplies the arguments needed to compute your Metric, and the `condition_domain_keys` tuple defines the domain on which the Metric is operating.

This is how the tuples appear in the MulticolumnMapMetrics Metric:

```python
condition_domain_keys = (
   "batch_id",       
   "table",       
   "column_list",       
   "row_condition",       
   "condition_parser",       
   "ignore_row_if",   
)   
condition_value_keys = ("sum_total",)

ColumnAggregateMetrics, TableMetrics, and QueryMetrics can similarly define a value_keys tuple or a domain_keys tuple:

value_keys = ("column",)
domain_keys = ("query",)
```

After the attributes are added to the Expectation and the Metric, the custom parameters can be passed into and utilized within the individual Metric functions. For example, this how it appears in the ColumnValuesBetween Metric:

```python
classColumnValuesBetween(ColumnMapMetricProvider):   
condition_metric_name = "column_values.between"   
condition_value_keys = (       
   "min_value",       
   "max_value",       
   "strict_min",       
   "strict_max",       
   "parse_strings_as_datetimes",
   "allow_cross_type_comparisons",
)
@column_condition_partial(engine=PandasExecutionEngine)   
def_pandas(       
   cls,       
   column,       
   min_value=None,       
   max_value=None,       
   strict_min=None,       
   strict_max=None,       
   parse_strings_as_datetimes: bool = False,
   allow_cross_type_comparisons=None,       
   **kwargs   
):
```
To view the full code that you would use to pass custom parameters in a Custom Expectation, see [expect_column_values_to_be_lat_lon_coordinates_in_range_of_given_point.py](https://github.com/great-expectations/great_expectations/blob/develop/contrib/great_expectations_geospatial_expectations/great_expectations_geospatial_expectations/expectations/expect_column_values_to_be_lat_lon_coordinates_in_range_of_given_point.py).

