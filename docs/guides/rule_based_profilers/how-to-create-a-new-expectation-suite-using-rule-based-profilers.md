---
title: How to create a new Expectation Suite using Rule Based Profilers
---
import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx';

<Prerequisites>

- Have configured a Datasource to connect to your data
- Have a basic understanding of [Metrics in Great Expectations](https://docs.greatexpectations.io/en/latest/reference/core_concepts/metrics.html)
- Have a basic understanding of [Expectation Configurations in Great Expectations](https://docs.greatexpectations.io/en/latest/reference/core_concepts/expectations/expectations.html#expectation-concepts-domain-and-success-keys)

</Prerequisites>

### Rule-Based Profilers

A [Profiler](https://docs.greatexpectations.io/en/latest/reference/core_concepts/profilers.html) uses a [Datasource](https://docs.greatexpectations.io/en/latest/reference/core_concepts/datasource.html) to build a new Expectation Suite. 

Rule-based profilers allow users to provide a highly configurable specification which is composed of rules to use in order to build an Expectation Suite by profiling existing data.

A rule in a rule-based profiler could say something like "Look at every column in a table, and if that column is numeric, add an `expect_column_values_to_be_between` expectation to my Expectation Suite, where the `min_value` for the expectation is the minimum value for the column, and the `max_value` for the expectation is the maximum value for the column."

If you had twelve months of taxi ride data in csvs, a rule in a rule-based profiler could say something like "Use the table row counts of these twelve months of data to estimate a table row count range and create an ExpectationConfiguration for `expect_table_row_count_to_be_between` with that row count range."

Each rule in a rule-based profiler has three types of components:
**1. DomainBuilders**: A DomainBuilder will inspect some data that you provide to the Profiler, and compile a list of Domains for which you would like to build expectations. 
**1. ParameterBuilders**: A ParameterBuilder will inspect some data that you provide to the Profiler, and compile a dictionary of Parameters that you can use when construction your ExpectationConfigurations
**1. ExpectationConfigurationBuilders**: An ExpectationConfigurationBuilder will take the Domains compiled by the DomainBuilder, and assemble ExpectationConfigurations using Parameters built by the ParameterBuilder

In addition to Rules, a rule-based profiler enables you to specify Variables, which are global and can be used in any of the Rules. For instance, you may want to reference the same BatchRequest or the same tolerance in multiple Rules, and declaring these as Variables will enable you to do so. 

Let's see how we could set up a Profiler YAML configuration to create a rule for our example above with twelve months of taxi data. First, we'll add in a `Variables` key and some Variables that we'll use. Next, we'll add a top level `rules` key, and then the name of your `rule`:
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L10-L15
```

After that, we'll add our DomainBuilder. In this case, we'll use a TableDomainBuilder, which will indicate that any expectations we build for this Domain will be at the Table level. Each Rule in our Profiler config can only use one DomainBuilder.
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L16-L17
```

Next, we'll use a NumericMetricRangeMultiBatchParameterBuilder to get an estimate to use for the `min_value` and `max_value` of our `expect_table_row_count_to_be_between` expectation. This ParameterBuilder will take in a BatchRequest consisting of the three Batches prior to our current Batch, and use the row counts of each of those months to get a probable range of row counts that you could use in your ExpectationConfiguration.
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L18-L32
```

A Rule can have multiple ParameterBuilders if needed, but in our case, we'll only use the one for now.

Finally, you would use an ExpectationConfigurationBuilder to actually build your `expect_table_row_count_to_be_between` expectation, where the Domain is the Domain returned by your TableDomainBuilder (your entire table), and the `min_value` and `max_value` are Parameters returned by your NumericMetricRangeMultiBatchParameterBuilder.
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L33-L41
```

When we put it all together, here is what our config with our single `row_count_rule` looks like:
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L10-L41
```

Now let's use our config to profile our data and create a simple Expectation Suite!

First we'll do some basic set-up - set up a DataContext and parse our YAML
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L100-L104
```

Next, we'll instantiate our Profiler, passing in our config and our DataContext
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L106-L109
```

Finally, we'll run `profiler.profile()` and save it to a variable. 
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L111
```
Then, we can print our suite so we can see how it looks!
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L116-L140
```

Let's add one more rule to our Rule-Based Profiler config. This Rule will use the DomainBuilder to populate a list of all of the numeric columns in one Batch of taxi data (in this case, the most recent Batch). It will then use our NumericMetricRangeMultiBatchParameterBuilder looking at the three Batches prior to our most recent Batch to get probable ranges for the min and max values for each of those columns. Finally, it will use those ranges to add two ExpectationConfigurations for each of those columns: `expect_column_min_to_be_between` and `expect_column_max_to_be_between`.

As before, we will first add the name of our rule, and then specify the DomainBuilder.
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L43-L53
```
In this case, our DomainBuilder configuration is a bit more complex. First, we are using a SimpleSemanticTypeColumnDomainBuilder. This will take a table, and return a list of all columns that match the `semantic_type` specified - `numeric` in our case.

Then, we need to specify a BatchRequest that returns exactly one Batch of data (this is our `data_connector_query` with `index` equal to `-1`). This tells us which Batch to use to get the columns from which we will select our numeric columns. Though we might hope that all our Batches of data have the same columns, in actuality, there might be differences between the Batches, and so we explicitly specify the Batch we want to use here.

After this, we specify our ParameterBuilders. This is very similar to the specification in our previous rule, except we will be specifying two NumericMetricRangeMultiBatchParameterBuilders to get a probable range for the `min_value` and `max_value` of each of our numeric columns. Thus one ParameterBuilder will take the `column.min` `metric_name`, and the other will take the `column.max` `metric_name`.
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L54-L78
```

Finally, we'll put together our `Domains` and `Parameters` in our `ExpectationConfigurationBuilders`
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L79-L97
```

Putting together our entire config, with both of our Rules, we get 
```yaml file=../../../tests/integration/docusaurus/rule_based_profiler/multi_batch_example.py#L9-L97
```

And if we re-instantiate and re-run our `Profiler`, we'll have an updated suite with a table row count expectation for our table, and column min and column max expectations for each of our numeric columns!


### Glossary of Terms
 Concept/Entity | Definition 
 -------|-------
**Profiling** | https://docs.greatexpectations.io/en/latest/reference/core_concepts.html#profilers
**Profiler Rule** | A RuleConfigurableProfiler (new style) uses one or more Profiler Rules.  A Profiler Rule creates Expectations for the configured Domains.  A Profiler Rule can be built using Parameters calculated at runtime or hard-coded (as Variables).  These Parameters can be local to a Domain or global to all Metric Domains.
**Metric** | https://docs.greatexpectations.io/en/latest/reference/core_concepts.html#expectations-and-metrics
**Domain** | https://docs.greatexpectations.io/en/latest/reference/core_concepts/expectations/expectations.html#expectation-concepts-domain-and-success-keys . An Expectation Domain may be different from a corresponding Metric Domain. An Expectation may compute metrics using domains different than the Expectation’s domain.  For example, the Domain for a two-column expectation such as expect_column_A_min_to_be_less_than_column_B_min is the pair of the given column names.  However, a Metric would be computed for each individual column (as a column type Domain), instead of a single metric using the Expectation’s domain.
**Domain Kwargs** | Domain Kwargs are keyword arguments (name-value pairs) that define a part of the Metric Domain with its specific value.  For example, if the MetricDomainType is “table” and the name of the table is “orders”, then metric_domain_kwargs = {“table”: “orders”}.  Similarly, if the MetricDomainType is “column” and the name of the column is “price”, then metric_domain_kwargs = {“column”: “price”}.  In this example, assuming that the column “price” contains floating point numbers, the Metric “column.mean” will accept the above  metric_domain_kwargs as an argument and return the mean value of the price column of the “orders” table.
**Metric Value Kwargs** | Metric Value Kwargs are keyword arguments (name-value pairs) that are used by Metrics in their functional implementations.  For the “orders” table and “price” column example, we could have metric_value_kwargs = {“skip_nan”: True} indicating that if the Metric computation encounters a NaN as a column value, it should ignore it (presumably having metric_value_kwargs = {“skip_nan”: False} will result in an Exception if a NaN value is encountered).
**Metric Value** | The result of computing a metric on a particular domain.
**DomainBuilder** | DomainBuilder is a base class for classes tasked with building the lists of Domain objects.  Each Domain object contains a Domain Kwargs dictionary and the “domain_type” property.  The Domain Kwargs dictionary must contain the “batch_id” key. For example, suppose that the Domain is a storage type Metric Domain “column”.  Then, as the first step, the corresponding DomainBuilder examines a Batch of data and finds the list of columns.  As a next step, this list of columns may be optionally filtered according to various criteria, such as data type, a pattern in the name (e.g., keep only column names with prefix/suffix, such as “_id”, or “_date”, or “_dt”, etc.), and other options.  
**DomainType** | The options variants of the DomainType are “schema”, “table”, and “column” – they are directly linked to the way data is laid out in common storage systems (databases, filesystems, etc.).  
**SemanticDomainType** | On the other hand, the “semantic” domain types carry certain meaning embedded in the way the particular domain is put together (e.g., to fit the needs of common use cases).  For instance, columns named “id”, “identity”, “key”, et al can be grouped under the “identity” domain; all columns that are integers and different precision floating point numbers can be placed under the “numeric” domain umbrella; all character and text column types can be represented by the “text” domain, and so on.  Moreover, multiple storage entities (e.g., columns) and entire schemas can be processed by viewing them as domains.  While certain such common semantic domain types have already been defined (e.g., “column_pair” and “multicolumn”), custom semantic domain types can be created with virtually no restrictions (e.g., the entire schema can be a semantic domain type).  The reason for this flexibility is to enable the creation of semantic domain types in a way that simplifies the implementation of a wide variety of profiling use cases.  Consequently, the lists of the DomainType members will likely be extended over time.
**Parameter** | Parameter is a dictionary object containing the keys “parameters” (flexible types) and “details” (a dictionary).  The Parameter (dictionary) is built and returned by an implementation of the ParameterBuilder class.  The specific values of these keys vary depending on the use case.  For example, the MetricParameterBuilder returns the result of evaluating (resolving) a Metric as the value corresponding to the “parameters” key and no value for the “details” key.  The SimpleDateFormatStringParameterBuilder returns the best fit for a data format string for a “column” type Domain with the “success_ratio” (a confidence measure) as the key in the “details” dictionary.
**Variable** | Variables are essentially Parameters that are globally scoped. Whereas Parameters can only be referenced within the Rule in which they were defined, Variables are defined outside of Rules, and can be referenced in any and all of the Rules in the config. Variables can be defined as constants, or be derived from metrics. For example, one could create an earliest_date variable by passing in a known constant and used as a min_valuewhen generating ExpectationConfigurations or one could create a latest_date variable that used a metric to return yesterday (the day before this profiler was run, or even the day before the validation is run).
**ParameterBuilder** | The ParameterBuilder base class is instantiated with the formal parameter name (i.e., using the “$parameter.category.subcatergory.name” syntax) and the DataContext reference.  The specific ParameterBuilder implementations may add other fields as needed for their particular purposes.  For instance, the MetricParameterBuilder accepts “metric_domain_kwargs” and “metric_value_kwargs”, both of which are needed in order to instantiate the “MetricConfiguration” class; the SimpleDateFormatStringParameterBuilder accepts candidate data format strings; and so on.  Whatever the specifics of a given ParameterBuilder, the output of the “build_parameters()” method is the ParameterContainer object with the structure that matches the syllables (or parts) of the (dot-separated) parameter name.
**ParameterContainer** | The ParameterContainer class implements the “topic tree” – a general name for parameters that use the “$parameter.category.subcatergory.name” syntax.  Each ParameterContainer object is a tree node, containing the dictionary of attribute name-value pairs and optional descendants.  Hence, each ParameterContainer object, including the descendants, points to a tree (or a subtree, which, in itself, is also – structurally -- a tree, albeit a shorter one).
**ExpectationConfigurationBuilder** | ExpectationConfigurationBuilder is instantiated with a flexible dictionary of arguments (kwargs), whose contents vary depending on the purpose.  For example, the most basic ExpectationConfigurationBuilder (called DefaultExpectationConfigurationBuilder) accepts the “expectation_type” (i.e., the name of the Expectation, which begins with “expect_”, from) and the mapping between the names of the configuration arguments that the particular Expectation expects and the corresponding formal parameter names (i.e., using the “$parameter.category.subcatergory.name” syntax).  The system then evaluates the formal parameters and the DefaultExpectationConfigurationBuilder builds the ExpectationConfiguration object for each domain.  While DefaultExpectationConfigurationBuilder is the only one available at the moment, more complex versions of the ExpectationConfigurationBuilder are envisioned, especially for multi-batch use cases.
