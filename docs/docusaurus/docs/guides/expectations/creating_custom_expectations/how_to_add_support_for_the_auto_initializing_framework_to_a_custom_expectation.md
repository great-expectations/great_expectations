---
title: Add support for the auto-initializing framework to a custom Expectation
---
import Prerequisites from '../creating_custom_expectations/components/prerequisites.jsx'

## Prerequisites

<Prerequisites>

- [A custom Expectation](./overview.md).

</Prerequisites>

## Steps

### 1. Determine if the auto-initializing framework is appropriate to include in your Expectation

Auto-initializing Expectations automate parameter estimation for Expectations, but not all parameters require this kind of estimation.  If your expectation only takes in a Domain (such as the name of a column) then it will not benefit from being configured to work in the auto-initializing framework.  In general, the auto-initializing Expectation framework benefits those Expectations that have numeric ranges which are intended to be descriptive of the data found in a Batch or Batches.  Existing examples of these would be Expectations such as `ExpectColumnMeanToBeBetween`, `ExpectColumnMaxToBeBetween`, or `ExpectColumnSumToBeBetween`.

### 2. Build a Custom Profiler for your Expectation

In order to automate the estimation of parameters, auto-initializing Expectations utilize a Custom Profiler.  You will need to create an appropriate configuration for the Profiler that your Expectation will use.  The easiest way to do this is to modify an existing Profiler configuration.

You can find existing Profiler configurations in the source code for any Expectation that works within the auto-initializing framework.  For this example, we will look at the existing configuration for the `ExpectColumnMeanToBeBetween` Expectation. You can [view the source code for this Expectation on our GitHub](https://github.com/great-expectations/great_expectations/blob/f53e27b068007471b819fc089f008d2a24864d20/great_expectations/expectations/core/expect_column_mean_to_be_between.py).

#### 2a. Modifying variables

Key-value pairs defined in the `variables` portion of a Profiler Configuration will be shared across all of its `Rules` and `Rule` components.  This helps you define and keep track of values without having to input them multiple times.  In our example, the `variables` are:

* `strict_min`: Used by `expect_column_mean_to_be_between` Expectation. Recognized values are `True` or `False`.
* `strict_max`: Used by `expect_column_mean_to_be_between` Expectation. Recognized values are `True` or `False`. 
* `false_positive_rate`: Used by `NumericMetricRangeMultiBatchParameterBuilder`. Typically, this will be a float `0 <= 1.0`.
* `quantile_statistic_interpolation_method`: Used by `NumericMetricRangeMultiBatchParameterBuilder`, which is used when estimating quantile values (not relevant in our case). Recognized values include `auto`, `nearest`, and `linear`.
* `estimator`: Used by `NumericMetricRangeMultiBatchParameterBuilder`. Recognized values include `oneshot`, `bootstrap`, and `kde`. 
* `n_resamples`:  Used by `NumericMetricRangeMultiBatchParameterBuilder`. Integer values are expected. 
* `include_estimator_samples_histogram_in_details`: Used by `NumericMetricRangeMultiBatchParameterBuilder`. Recognized values are `True` or `False`.
* `truncate_values`: A value used by the `NumericMetricRangeMultiBatchParameterBuilder` to specify the `[lower_bound, upper_bound]` interval, where either boundary is numeric or None. In our case the value is an empty dictionary, and an equivalent configuration would have been `truncate_values : { lower_bound: None, upper_bound: None }`. 
* `round_decimals` : Used by `NumericMetricRangeMultiBatchParameterBuilder`, and determines how many digits after the decimal point to output (in our case 2). 

#### 2b. Modifying the `domain_builder`

The `DomainBuilder` configuration requries a `class_name` and `module_name`.  In this example, we will be using the `ColumnDomainBuilder` which outputs the column of interest (for example: `trip_distance` in the NYC taxi data) which is then accessed by the `ExpectationConfigurationBuilder` using the variable `$domain.domain_kwargs.column`.

- **`class_name`:** is the name of the DomainBuilder class that is to be used.  Additional Domain Builders are:
  - `ColumnDomainBuilder`: This `DomainBuilder` outputs column Domains, which are required by `ColumnAggregateExpectations` like (`expect_column_median_to_be_between`).
  - `MultiColumnDomainBuilder`: This DomainBuilder outputs `multicolumn` Domains by taking in a column list in the `include_column_names` parameter.
  - `ColumnPairDomainBuilder`: This DomainBuilder outputs columnpair domains by taking in a column pair list in the include_column_names parameter.
  - `TableDomainBuilder`: This `DomainBuilder` outputs table `Domains`, which is required by `Expectations` that act on tables, like (`expect_table_row_count_to_equal`, or `expect_table_columns_to_match_set`).
  - `MapMetricColumnDomainBuilder`: This `DomainBuilder` allows you to choose columns based on Map Metrics, which give a yes/no answer for individual values or rows.
  - `CategoricalColumnDomainBuilder`: This `DomainBuilder` allows you to choose columns based on their cardinality (number of unique values).
  :::note
  `CategoricalColumnDomainBuilder` will take in various `cardinality_limit_mode` values for cardinality. For a full listing of valid modes, along with the associated values, please refer to [the `CardinalityLimitMode` enum in the source code on our GitHub](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/rule_based_profiler/helpers/cardinality_checker.py).
  :::

- **`module_name`**: is `great_expectations.rule_based_profiler.domain_builder`, which is common for all `DomainBuilders`.

#### 2c. Modifying the `ParameterBuilder`

Our example contains a configuration for one `ParamterBuilder`, a `NumericMetricRangeMultiBatchParameterBuilder`.  You can find the other types of `ParameterBuilder` by browsing [the source code in our GitHub](https://github.com/great-expectations/great_expectations/tree/develop/great_expectations/rule_based_profiler/parameter_builder).  For the `NumericMetricRangeMultiBatchParameterBuilder` the configuration key-value pairs consist of:

* `name`: an arbitrary name assigned to this `ParameterBuilder` configuration.
* `class_name`: the name of the class that corresponds to the `ParameterBuilder` defined by this configuration.
* `module_name`: `great_expectations.rule_based_profiler.parameter_builder` which is the same for all `ParameterBuilders`.
* `json_serialize`: Boolean value that determines whether to convert computed value to JSON prior to saving result. 
* `estimator`: choice of the estimation algorithm: "oneshot" (one observation), "bootstrap" (default), or "kde" (kernel density estimation). Value is pulled from `$variables.estimator`, which is set to "bootstrap" in our configuration.  
* `quantile_statistic_interpolation_method`:  Applicable for the "bootstrap" sampling method. Determines the value of interpolation "method" to `np.quantile()` statistic, which is used for confidence intervals. Value is pulled from `$variables.quantile_statistic_interpolation_method`, which is set to "auto" in our configuration.
* `enforce_numeric_metric`: used in `MetricConfiguration` to ensure that metric computations return numeric values. Set to `True`. 
* `n_resamples`: Applicable for the "bootstrap" and "kde" sampling methods -- if omitted (default), then 9999 is used.  Value is pulled from `$variables.n_resamples`, which is set to `9999` in our configuration.
* `round_decimals`: User-configured non-negative integer indicating the number of decimals of the rounding precision of the computed parameter values (i.e., `min_value`, `max_value`) prior to packaging them on output.  If omitted, then no rounding is performed, unless the computed value is already an integer. Value is pulled from `$variables.round_decimals` which is `2` in our configuration.
* `reduce_scalar_metric`: If `True` (default), then reduces computation of 1-dimensional metric to scalar value. This value is set to `True`.
* `include_estimator_samples_histogram_in_details`: For the "bootstrap" sampling method -- if True, then add 10-bin histogram of bootstraps to "details"; otherwise, omit this information (default). Value pulled from `$variables.include_estimator_samples_histogram_in_details`, which is `False` in our configuration.
* `truncate_values`: User-configured directive for whether or not to allow the computed parameter values (i.e.,`lower_bound`, `upper_bound`) to take on values outside the specified bounds when packaged on output. Value pulled from `$variables.truncate_values`, which is `None` in our configuration.
* `false_positive_rate`: User-configured fraction between 0 and 1 expressing desired false positive rate for identifying unexpected values as judged by the upper- and lower- quantiles of the observed metric data. Value pulled from `$variables.false_positive_rate` and is `0.05` in our configuration.
* `replace_nan_with_zero`: If False, then if the computed metric gives `NaN`, then exception is raised; otherwise, if True (default), then if the computed metric gives NaN, then it is converted to the 0.0 (float) value. Set to `True` in our configuration.
* `metric_domain_kwargs`: Domain values for `ParameteBuilder`. Pulled from `$domain.domain_kwargs`, and is empty in our configuration.

#### 2d. Modifing the `expectation_configuration_builders`

Our Configuration contains 1 `ExpectationConfigurationBuilder`, for the `expect_column_mean_to_be_between` Expectation type. 

The `ExpectationConfigurationBuilder` configuration requires a `expectation_type`, `class_name` and `module_name`:

* `expectation_type`: `expect_column_mean_to_be_between`
* `class_name`: `DefaultExpectationConfigurationBuilder`
* `module_name`: `great_expectations.rule_based_profiler.expectation_configuration_builder` which is common for all `ExpectationConfigurationBuilders`

Also included are:

* `validation_parameter_builder_configs`: Which are a list of `ValidationParameterBuilder` configurations, and our configuration case contains the `ParameterBuilder` described in the previous section. 

Next are the parameters that are specific to the `expect_column_mean_to_be_between` `Expectation`.

* `column`: Pulled from `DomainBuilder` using the parameter`$domain.domain_kwargs.column`
* `min_value`:  Pulled from the `ParameterBuilder` using `$parameter.mean_range_estimator.value[0]`
* `max_value`: Pulled from the `ParameterBuilder` using `$parameter.mean_range_estimator.value[1]`
* `strict_min`: Pulled from ``$variables.strict_min`, which is `False`. 
* `strict_max`: Pulled from ``$variables.strict_max`, which is `False`. 

Last is `meta` which contains `details` from our `parameter_builder`. 

### 3. Assign your configuration to the `default_profiler_config` class attribute of your Expectation

Once you have modified the necessary parts of the Profiler configuration to suit your purposes you will need to assign it to the `default_profiler_config` class attribute of your Expectation.  If you initially copied the Profiler configuration that you modified from another Expectation that was already set up to work with the auto-initializing framework then you can refer to that Expectation for an example of this.

### 4. Test your Expectation with `auto=True`

After assigning your Profiler configuration to the `default_profiler_config` attribute of your Expectation, your Expectation should be able to work in the auto-initializing framework.  [Test your expectation](./how_to_use_custom_expectations.md) with the parameter `auto=True`.
