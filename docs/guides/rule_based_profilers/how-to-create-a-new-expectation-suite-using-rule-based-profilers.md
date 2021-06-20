Profilers

https://docs.greatexpectations.io/en/latest/reference/core_concepts.html#profilers

Pre-requisites: 

Metrics

Domains

Expectation Configurations

Batches

A Profiler uses an Execution Engine to build a new Expectation Suite. It can use zero, one, or more Batches of data to decide which Expectations to include in the new Suite. It can use other information, like a schema or an idea. A profiler may be used to create basic high-level expectations based on a schema even without data, to create specific Expectations based on team conventions or statistical properties in a dataset, or even to generate Expectation Suites specifically designed to be rendered by a Descriptive Renderer for data discovery.

https://docs.greatexpectations.io/en/latest/reference/core_concepts/data_discovery.html
https://docs.greatexpectations.io/en/latest/reference/core_concepts/profilers.html

https://superconductive.atlassian.net/wiki/spaces/GE/pages/38535396/2021-04-19+Profiling+Concepts+Taxonomy

Rule-based profilers allow users to provide a highly configurable specification in YAML form which is composed of rules to use in order to build an Expectation Suite by profiling existing data.

A rule in a rule-based profiler could say something like "Look at every column in a table, and if that column ends is numeric, add an `expect_column_values_to_be_between` expectation to my Expectation Suite, where the `min_value` for the expectation is the minimum value for the column, and the `max_value` for the expectation is the maximum value for the column."

If you had twelve months of taxi ride data in csvs, a rule in a rule-based profiler could say something like "Use the table row counts of these twelve months of data to estimate a table row count range and create an ExpectationConfiguration for `expect_table_row_count_to_be_between` with that row count range."

Each rule in a rule-based profiler has three types of components:
1. DomainBuilders: A DomainBuilder will inspect some data that you provide to the Profiler, and compile a list of Domains for which you would like to build expectations. 
1. ParameterBuilders: A ParameterBuilder will inspect some data that you provide to the Profiler, and compile a dictionary of Parameters that you can use when construction your ExpectationConfigurations
1. ExpectationConfigurationBuilders: An ExpectationConfigurationBuilder will take the Domains compiled by the DomainBuilder, and assemble ExpectationConfigurations using Parameters built by the ParameterBuilder

In addition to Rules, a rule-based profiler enables you to specify Variables, which are global and can be used in any of the Rules. For instance, you may want to reference the same BatchRequest or the same tolerance in multiple Rules, and declaring these as Variables will enable you to do so. 

Let's see how we could set up a Profiler YAML configuration to create a rule for our example above with twelve months of taxi data. First, we'll add in a Variables key and some Variables that we'll use. Next, we'll add a top level `rules` key, and then the name of your `rule`:
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L21-L26
```

After that, we'll add our DomainBuilder. In this case, we'll use a TableDomainBuilder, which will indicate that any expectations we build for this Domain will be at the Table level. Each Rule in our Profiler config can only use one DomainBuilder.
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L27-L28
```

Next, we'll use a NumericMetricRangeMultiBatchParameterBuilder to get an estimate to use for the `min_value` and `max_value` of our `expect_table_row_count_to_be_between` expectation. This ParameterBuilder will take in a BatchRequest consisting of the twelve months of data from 2020, and use the row counts of each of those months to get a probable range of row counts that you could use in your ExpectationConfiguration.
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L29-L44
```

A Rule can have multiple ParameterBuilders if needed, but in our case, we'll only use the one for now.

Finally, you would use an ExpectationConfigurationBuilder to actually build your `expect_table_row_count_to_be_between` expectation, where the Domain is the Domain returned by your TableDomainBuilder (your entire table), and the `min_value` and `max_value` are Parameters returned by your NumericMetricRangeMultiBatchParameterBuilder.
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L45-L53
```

When we put it all together, here is what our config with our single `row_count_rule` looks like:
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L21-L53
```

Now let's use our config to profile our data and create an Expectation Suite!

First we'll do some basic set-up - set up a DataContext and parse our YAML
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L98-L102
```

Next, we'll instantiate our Profiler, passing in our config and our DataContext
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L120-L125
```

Finally, we'll run `profiler.profile()` and save it to a variable
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L127-L128
```