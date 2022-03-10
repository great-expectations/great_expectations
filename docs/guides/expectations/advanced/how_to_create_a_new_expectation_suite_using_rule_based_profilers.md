---
title: How to create a new Expectation Suite using Rule Based Profilers
---
import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';

In this tutorial, you will develop hands-on experience with configuring a Rule-Based Profiler to create an Expectation Suite. You will Profile several batches of NYC yellow taxi trip data to come up with reasonable estimates for the ranges of expectations for several numeric columns.

:::warning

Please note that Rule Based Profiler is currently undergoing development and is considered an experimental feature.
While the contents of this document accurately reflect the state of the feature, they are susceptible to change.

:::

<Prerequisites>

- Have a basic understanding of [Metrics in Great Expectations](https://docs.greatexpectations.io/docs/reference/metrics)
- Have a basic understanding of [Expectation Configurations in Great Expectations](https://docs.greatexpectations.io/docs/reference/expectations/expectations)
- Have read the sections in Core Concepts on [Profilers](../../../reference/profilers.md) and [Rule-Based Profilers](../../../reference/profilers.md#rule-based-profilers)

</Prerequisites>


## Steps
### 1. Create a new Great Expectations project
- Create a new directory, called `taxi_profiling_tutorial`
- Within this directory, create another directory called `data`
- Navigate to the top level of `taxi_profiling_tutorial` in a terminal and run `great_expectations init`

### 2. Download the data
- Download [this directory](https://github.com/great-expectations/great_expectations/tree/develop/tests/test_sets/taxi_yellow_tripdata_samples) of yellow taxi trip `csv` files from the Great Expectations GitHub repo. You can use a tool like [DownGit](https://downgit.github.io/) to do so
- Move the unzipped directory of `csv` files into the `data` directory that you created in Step 1

### 3. Setting up your Datasource
- Follow the steps in the [How to connect to data on a filesystem using Pandas](../../../guides/connecting_to_your_data/filesystem/pandas.md). For the purpose of this tutorial, we will work from a `yaml` to set up your datasource config. When you open up your notebook to create and test and save your datasource config, replace the config docstring with the following docstring:
```python
example_yaml = f"""
name: taxi_pandas
class_name: Datasource
execution_engine:
  class_name: PandasExecutionEngine
data_connectors:
  monthly:
    base_directory: ../<YOUR BASE DIR>/
    glob_directive: '*.csv'
    class_name: ConfiguredAssetFilesystemDataConnector
    assets:
      my_reports:
        base_directory: ./
        group_names:
          - name
          - year
          - month
        class_name: Asset
        pattern: (.+)_(\d.*)-(\d.*)\.csv
"""
```
- Test your YAML config to make sure it works - you should see some of the taxi `csv` filenames listed
- Save your datasource config

### 4. Configuring the Profiler
- Now, we'll create a new script in the same top-level `taxi_profiling_tutorial` directory called `profiler_script.py`. If you prefer, you could open up a Jupyter Notebook and run this there instead.
- At the top of this file, we will create a new YAML docstring assigned to a variable called `profiler_config`. This will look similar to the YAML docstring we used above when creating our Datasource. Over the next several steps, we will slowly add lines to this docstring by typing or pasting in the lines below
```python 
profiler_config = """

"""
```

First, we'll add some relevant top level keys (`name` and `config_version`) to label our profiler and associate it with a specific version of the feature.
```yaml file=../../../../tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py#L10-L12
```

:::info Config Versioning

Note that at the time of writing this document, `1.0` is the only supported config version.

:::

Then, we'll add in a `Variables` key and some Variables that we'll use. Next, we'll add a top level `rules` key, and then the name of your `rule`:
```yaml file=../../../../tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py#L13-L15
```

After that, we'll add our DomainBuilder. In this case, we'll use a TableDomainBuilder, which will indicate that any expectations we build for this Domain will be at the Table level. Each Rule in our Profiler config can only use one DomainBuilder.
```yaml file=../../../../tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py#L19-L20
```

Next, we'll use a NumericMetricRangeMultiBatchParameterBuilder to get an estimate to use for the `min_value` and `max_value` of our `expect_table_row_count_to_be_between` expectation. This ParameterBuilder will take in a BatchRequest consisting of the five Batches prior to our current Batch, and use the row counts of each of those months to get a probable range of row counts that you could use in your ExpectationConfiguration.
```yaml file=../../../../tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py#L21-L35
```

A Rule can have multiple ParameterBuilders if needed, but in our case, we'll only use the one for now.

Finally, you would use an ExpectationConfigurationBuilder to actually build your `expect_table_row_count_to_be_between` expectation, where the Domain is the Domain returned by your TableDomainBuilder (your entire table), and the `min_value` and `max_value` are Parameters returned by your NumericMetricRangeMultiBatchParameterBuilder.
```yaml file=../../../../tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py#L36-L44
```
You can see here that we use a special `$` syntax to reference `variables` and `parameters` that have been previously defined in our config. You can see a more thorough description of this syntax in the  docstring for [`ParameterContainer` here](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/rule_based_profiler/types/parameter_container.py).

- When we put it all together, here is what our config with our single `row_count_rule` looks like:
```yaml file=../../../../tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py#L10-L44
```

### 5. Running the Profiler
Now let's use our config to profile our data and create a simple Expectation Suite!

First we'll do some basic set-up - set up a DataContext and parse our YAML
```yaml file=../../../../tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py#L102-L106
```

Next, we'll instantiate our Profiler, passing in our config and our DataContext
```yaml file=../../../../tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py#L107-L114
```

Finally, we'll run `profile()` and save it to a variable. 
```yaml file=../../../../tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py#L115
```
Then, we can print our suite so we can see how it looks!
```yaml file=../../../../tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py#L120-L138
```

### 6. Adding a Rule for Columns
Let's add one more rule to our Rule-Based Profiler config. This Rule will use the DomainBuilder to populate a list of all of the numeric columns in one Batch of taxi data (in this case, the most recent Batch). It will then use our NumericMetricRangeMultiBatchParameterBuilder looking at the five Batches prior to our most recent Batch to get probable ranges for the min and max values for each of those columns. Finally, it will use those ranges to add two ExpectationConfigurations for each of those columns: `expect_column_min_to_be_between` and `expect_column_max_to_be_between`. This rule will go directly below our previous rule.

As before, we will first add the name of our rule, and then specify the DomainBuilder.
```yaml file=../../../../tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py#L45-L56
```
In this case, our DomainBuilder configuration is a bit more complex. First, we are using a SimpleSemanticTypeColumnDomainBuilder. This will take a table, and return a list of all columns that match the `semantic_type` specified - `numeric` in our case.

Then, we need to specify a BatchRequest that returns exactly one Batch of data (this is our `data_connector_query` with `index` equal to `-1`). This tells us which Batch to use to get the columns from which we will select our numeric columns. Though we might hope that all our Batches of data have the same columns, in actuality, there might be differences between the Batches, and so we explicitly specify the Batch we want to use here.

After this, we specify our ParameterBuilders. This is very similar to the specification in our previous rule, except we will be specifying two NumericMetricRangeMultiBatchParameterBuilders to get a probable range for the `min_value` and `max_value` of each of our numeric columns. Thus one ParameterBuilder will take the `column.min` `metric_name`, and the other will take the `column.max` `metric_name`.
```yaml file=../../../../tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py#L57-L81
```

Finally, we'll put together our `Domains` and `Parameters` in our `ExpectationConfigurationBuilders`
```yaml file=../../../../tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py#L82-L100
```

Putting together our entire config, with both of our Rules, we get 
```yaml file=../../../../tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py#L9-L100
```

And if we re-instantiate our `Profiler` with our config which now has two rules, and then we re-run the `Profiler`, we'll have an updated suite with a table row count expectation for our table, and column min and column max expectations for each of our numeric columns!

🚀Congratulations! You have successfully Profiled multi-batch data using a Rule-Based Profiler. Now you can try adding some new rules, or running your Profiler on some other data (remember to change the BatchRequest in your config)!🚀

## Additional Notes

To view the full script used in this page, see it on GitHub:

- [multi_batch_rule_based_profiler_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py)
