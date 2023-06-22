---
title: Create an Expectation Suite with a Custom Profiler
---
import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

In this tutorial, you will develop hands-on experience with configuring a Custom Profiler <TechnicalTag tag="profiler" text="Profiler" /> to create an <TechnicalTag tag="expectation_suite" text="Expectation Suite" />. You will <TechnicalTag tag="profiling" text="Profile" /> several <TechnicalTag tag="batch" text="Batches" /> of NYC yellow taxi trip data to come up with reasonable estimates for the ranges of <TechnicalTag tag="expectation" text="Expectations" /> for several numeric columns.

## Prerequisites

<Prerequisites>

- A basic understanding of <TechnicalTag tag="metric" text="Metrics" /> in Great Expectations.
- A basic understanding of [Expectation Configurations in Great Expectations](https://docs.greatexpectations.io/docs/reference/expectations/expectations).
- Completion of the overview of <TechnicalTag tag="profiler" text="Profilers" /> and the  [Custom Profilers](../../../terms/profiler.md#rule-based-profilers) section.

</Prerequisites>


## Create a new Great Expectations project

- Create a new directory, called `taxi_profiling_tutorial`
- Within this directory, create another directory called `data`

## Download the data

- Download [this directory](https://github.com/great-expectations/great_expectations/tree/develop/tests/test_sets/taxi_yellow_tripdata_samples) of yellow taxi trip `csv` files from the Great Expectations GitHub repo. You can use a tool like [DownGit](https://downgit.github.io/) to do so
- Move the unzipped directory of `csv` files into the `data` directory that you created in Step 1

## Create a context and add your Datasource

- See [How to connect to data on a filesystem using Pandas](/docs/0.15.50/guides/connecting_to_your_data/filesystem/pandas). Run the following command to add a Pandas Filesystem asset for the taxi data.

```python name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py init"
```

## Configure the Custom Profiler

- Now, we'll create a new script in the same top-level `taxi_profiling_tutorial` directory called `profiler_script.py`. If you prefer, you could open up a Jupyter Notebook and run this there instead.
- At the top of this file, we will create a new YAML docstring assigned to a variable called `profiler_config`. This will look similar to the YAML docstring we used above when creating our Datasource. Over the next several steps, we will slowly add lines to this docstring by typing or pasting in the lines below:

```python 
profiler_config = """

"""
```

First, we'll add some relevant top level keys (`name` and `config_version`) to label our Profiler and associate it with a specific version of the feature.

```yaml name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py name and config_version"
```

:::info Config Versioning

Note that at the time of writing this document, `1.0` is the only supported config version.

:::

Then, we'll add in a `Variables` key and some variables that we'll use. Next, we'll add a top level `rules` key, and then the name of your `rule`:

```yaml name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py variables and rule name"
```

After that, we'll add our Domain Builder. In this case, we'll use a `TableDomainBuilder`, which will indicate that any expectations we build for this Domain will be at the Table level. Each Rule in our Profiler config can only use one Domain Builder.

```yaml name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py row_count_rule domain_builder"
```

Next, we'll use a `NumericMetricRangeMultiBatchParameterBuilder` to get an estimate to use for the `min_value` and `max_value` of our `expect_table_row_count_to_be_between` Expectation. This Parameter Builder will take in a <TechnicalTag tag="batch_request" text="Batch Request" /> consisting of the five Batches prior to our current Batch, and use the row counts of each of those months to get a probable range of row counts that you could use in your `ExpectationConfiguration`.

```yaml name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py row_count_rule parameter_builders"
```

A Rule can have multiple `ParameterBuilders` if needed, but in our case, we'll only use the one for now.

Finally, you would use an `ExpectationConfigurationBuilder` to actually build your `expect_table_row_count_to_be_between` Expectation, where the Domain is the Domain returned by your `TableDomainBuilder` (your entire table), and the `min_value` and `max_value` are Parameters returned by your `NumericMetricRangeMultiBatchParameterBuilder`.

```yaml name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py row_count_rule expectation_configuration_builders"
```
You can see here that we use a special `$` syntax to reference `variables` and `parameters` that have been previously defined in our config. You can see a more thorough description of this syntax in the  docstring for [`ParameterContainer` here](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/rule_based_profiler/types/parameter_container.py).

- When we put it all together, here is what our config with our single `row_count_rule` looks like:

```yaml name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py full row_count_rule"
```

## Run the Custom Profiler

Now let's use our config to Profile our data and create an Expectation Suite!

First, we load the profiler config and instantiate our Profiler, passing in our config and our Data Context

```python name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py instantiate"
```

Then we run the profiler and save the result to a variable. 

```python name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py run"
```

Now we can print our Expectation Suite so we can see how it looks!

```python name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py row_count_rule_suite"
```

## Add a Rule for Columns

Let's add one more rule to our Profiler config. This Rule will use the `DomainBuilder` to populate a list of all of the numeric columns in one Batch of taxi data (in this case, the most recent Batch). It will then use our `NumericMetricRangeMultiBatchParameterBuilder` looking at the five Batches prior to our most recent Batch to get probable ranges for the min and max values for each of those columns. Finally, it will use those ranges to add two `ExpectationConfigurations` for each of those columns: `expect_column_min_to_be_between` and `expect_column_max_to_be_between`. This rule will go directly below our previous rule.

As before, we will first add the name of our rule, and then specify the `DomainBuilder`.

```yaml name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py column_ranges_rule domain_builder"
```

In this case, our `DomainBuilder` configuration is a bit more complex. First, we are using a `SimpleSemanticTypeColumnDomainBuilder`. This will take a table, and return a list of all columns that match the `semantic_type` specified - `numeric` in our case.

Then, we need to specify a Batch Request that returns exactly one Batch of data (this is our `data_connector_query` with `index` equal to `-1`). This tells us which Batch to use to get the columns from which we will select our numeric columns. Though we might hope that all our Batches of data have the same columns, in actuality, there might be differences between the Batches, and so we explicitly specify the Batch we want to use here.

After this, we specify our `ParameterBuilders`. This is very similar to the specification in our previous rule, except we will be specifying two `NumericMetricRangeMultiBatchParameterBuilders` to get a probable range for the `min_value` and `max_value` of each of our numeric columns. Thus one `ParameterBuilder` will take the `column.min` `metric_name`, and the other will take the `column.max` `metric_name`.

```yaml name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py column_ranges_rule parameter_builders"
```

Finally, we'll put together our `Domains` and `Parameters` in our `ExpectationConfigurationBuilders`:

```yaml name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py column_ranges_rule expectation_configuration_builders"
```

Putting together our entire config, with both of our Rules, we get:

```yaml name="tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py full profiler_config"
```

And if we re-instantiate our `Profiler` with our config which now has two rules, and then we re-run the `Profiler`, we'll have an updated Expectation Suite with a table row count Expectation for our table, and column min and column max Expectations for each of our numeric columns!

🚀Congratulations! You have successfully Profiled multi-batch data using a Custom Profiler. Now you can try adding some new Rules, or running your Profiler on some other data (remember to change the `BatchRequest` in your config)!🚀

## Additional Notes

To view the full script used in this page, see it on GitHub:

- [multi_batch_rule_based_profiler_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py)
