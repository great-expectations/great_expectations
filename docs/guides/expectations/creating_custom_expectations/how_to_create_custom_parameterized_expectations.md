---
title: How to create Custom Parameterized Expectations
---
import Prerequisites from '../creating_custom_expectations/components/prerequisites.jsx'

This guide will walk you through the process of creating Parameterized Expectations - very quickly. This method is only available using the new Modular Expectations API introduced in 0.13.

<Prerequisites>

- Read the [overview for creating Custom Expectations](./overview.md).

</Prerequisites>

A Parameterized [**Expectation**](../../../reference/expectations/expectations.md) is a capability unlocked by Modular Expectations. Now that Expectations are structured in class form, it is easy to inherit from these classes and build similar Expectations that are adapted to your own needs.

## Steps

### 1. Select an Expectation to inherit from

For the purpose of this exercise, we will implement the Expectations `expect_column_mean_to_be_positive` and `expect_column_values_to_be_two_letter_country_code` - realistic Expectations 
of the data that can easily inherit from `expect_column_mean_to_be_between` and `expect_column_values_to_be_in_set` respectively.

### 2. Select default values for your class

Our first implementation will be `expect_column_mean_to_be_positive`.

As can be seen in the implementation below, we have chosen to keep our default minimum value at 0, given that we are validating that all our values are positive. Setting the upper bound to `None` means that no upper bound will be checked – effectively setting the threshold at ∞ and allowing any positive value.

Notice that we do not need to set `default_kwarg_values` for all kwargs: it is sufficient to set them only for ones for which we would like to set a default value. To keep our implementation simple, we do not override the `metric_dependencies` or `success_keys`.

```python file=../../../../tests/expectations/core/test_expect_column_mean_to_be_positive.py#L12-L18
```

We could also explicitly override our parent methods to modify the behavior of our new Expectation, for example by updating the configuration validation to require the values we set as defaults not be altered.

```python file=../../../../tests/expectations/core/test_expect_column_mean_to_be_positive.py#L20-L25
```

Now for `expect_column_values_to_be_in_set`.

In this case, we will only be changing our `value_set`:

```python file=../../../../tests/expectations/core/test_expect_column_values_to_be_in_set.py#L14-L17
```

That's all there is to it - really!

<div style={{"text-align":"center"}}>  
<p style={{"color":"#8784FF","font-size":"1.4em"}}><b>  
Congratulations!<br/>&#127881; You've just built your first Parameterized Custom Expectation! &#127881;  
</b></p>  
</div>

### 3. Contribution (Optional)

If you plan to contribute your Expectation to the public open source project, you should include a `library_metadata` object. For example:

```python file=../../../../tests/expectations/core/test_expect_column_mean_to_be_positive.py#L27
```

This is particularly important because ***we*** want to make sure that ***you*** get credit for all your hard work!

Additionally, you will need to implement some basic examples and test cases before your contribution can be accepted. For guidance on examples and testing, see our [guide on implementing examples and test cases](./docs/guides/expectations/features_custom_expectations/how_to_add_example_cases_for_an_expectation.md).

:::note
For more information on our code standards and contribution, see our guide on [Levels of Maturity](../../../contributing/contributing_maturity.md#contributing-expectations) for Expectations.
:::
