---
title: How to configure a DataConnector to introspect and partition a file system or blob store
---
import WhereToRunCode from './components/where_to_run_code.md'
import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx'

This guide will help you introspect and partition any files type data store (e.g., filesystem, cloud blob storage) using
the different types of the active `Data Connector`.  For background, please see the `Datasource` specific guides in the
"Connecting to your data" section.

The file based data introspection and partitioning mechanisms in Great Expectations are useful for:
- Exploring the types, subdirectory location, and filepath naming structures of the files in your dataset, and
- Organizing the discovered files into `Data Assets` according to the identified structures.

Partitioning enables you to select the desired subsets of your dataset for [Validation](/docs/reference/validation).

<Prerequisites>

- [Configured and loaded a Data Context](../../tutorials/getting_started/initialize_a_data_context.md)
- [Configured a Datasource and Data Connector](../../reference/datasources.md)
  
</Prerequisites>

This guide will use the following `Datasource` configuration as an example:
```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L10-L48
```

To learn more about `Datasources`, `Data Connectors`, and `Batch(es)`, please see our [Datasources Core Concepts Guide](../../reference/datasources.md) in the [Core Concepts reference guide](../../reference/core_concepts.md). 

## Preliminary Steps

### 1. Choose how to run the code in this guide

<WhereToRunCode />

### 2. Instantiate your project's DataContext

Import these necessary packages and modules.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L1-L6
```

Load your DataContext into memory using the `get_context()` method.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L8
```

### 3. Configure your Datasource

Using the above example configuration, add in the path to a directory that contains your data.  Then run this code to
test your configuration:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L56
```

Given that the `glob_directive` in the example configuration is `*.csv`, if you specified a directory containing CSV
files, then you will see them listed as `Available data_asset_names` in the output of `test_yaml_config()`.

Feel free to adjust your configuration and re-run `test_yaml_config()` as needed.

### 4. Save the Datasource configuration to your DataContext

Save the configuration into your `DataContext` by using the `add_datasource()` function.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L58
```

## Introspection

### 1. Get names of available Data Assets
 
Getting names of available data assets using an `Inferred Asset Data Connector` gives you visibility into types and
naming structures of files in your filesystem or blob storage:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L59-L65
```

### 2.  Examine a few rows of a file

Pick a `data_asset_name` from the previous step and specify it in the `BatchRequest`:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L69-L73
```

Then load data into the `Validator` and print a brief excerpt of the file's contents (`n_rows = 5` is the default, but
you may look at more rows e.g. `validator.head(n_rows=100)`):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L79-L85
```

At this point, you can also perform additional checks, such as confirming the number of batches and the size of a batch.
For example (be sure to adjust this code to match the specifics of your data and configuration):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L87-L89
```

## Partitioning

Now use the `Configured Asset Data Connector` to gradually apply structure to the discovered assets and partition them
according to this structure.

### 1. Partition only by file name and type
 
You can employ a data asset that reflects a relatively general file structure (e.g., `taxi_data_flat` in the example
configuration) to represent files in a directory, which contain a certain prefix (e.g., `yellow_trip_data_sample_`) and
whose contents are of the desired type (e.g., CSV).

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L92-L96
```

For the present example, set `data_asset_name` to `taxi_data_flat` in the above `BatchRequest` specification.
(Customize for your own data set, as appropriate.)

Perform the relevant checks, such as confirm the number of batches and the size of a batch.
For example (be sure to adjust this code to match the specifics of your data and configuration):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L102-L104
```

### 2. Partition by year and month

Next, in recognition of a finer observed file path structure, you can refine the partitioning strategy.  For instance,
the `taxi_data_year_month` in our example configuration identifies three parts of a file path: `name` (as in "company
name"), `year`, and `month`.  This partitioning affords a rich set of filtering capabilities ranging from specifying the
exact values of the file name structure's components to allowing Python functions for implementing custom criteria.

To illustrate (using the present configuration example), set `data_asset_name` to `taxi_data_year_month` in the
following `BatchRequest` specification (customize for your own data set, as appropriate):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L108-L113
```

To obtain the data for the nine months of February through October, apply the following custom filter:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L118-L121
```

Now, perform the relevant checks, such as confirm the expected number of batches was retrieved, and the size of a batch:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L123-L125
```

You can then identify a particular batch (e.g., corresponding to the year and month of interest) and retrieve it for
data analysis as follows:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L129-L140
```

Note that in the present example, there can be up to three `BATCH_FILTER_PARAMETER` key-value pairs, because the regular
expression for the data asset `taxi_data_year_month` defines three groups: `name`, `year`, and `month`.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L145-L148
```

(Be sure to adjust the above code snippets to match the specifics of your data and configuration.)

Now, perform the relevant checks, such as confirm the expected number of batches was retrieved, and the size of a batch:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L150-L152
```

:::note
Omitting the `batch_filter_parameters` key from the `data_connector_query` will be interpreted in the least restrictive
(most broad) query, resulting in the largest number of `Batch` objects to be returned.
:::

### 3. Splitting and Sampling

Additional `Partitioning` mechanisms provided by Great Expectations include `Splitting` and `Sampling`.

`Splitting` provides the means of focusing the batch data on the values of certain dimensions of the data of interest.
To configure `Splitting`, specify a dimension (i.e., `column_name` or `column_names`), the method of splitting, and
parameters to be used by the specified splitting method.

`Sampling`, in turn, provides a means for reducing the amount of data in the retrieved batch to facilitate data analysis.
To configure `Sampling`, specify a dimension (i.e., `column_name` or the entire `table`), the method of sampling, and
parameters to be used by the specified sampling method.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L158-L186
```
```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L191-L194
```

For the present example, we can split according to the "passenger_count" column with the focus on two-passenger rides:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L195-L199
```

We can then obtain a random `10%` of the rows in the batch:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L200-L201
```

Finally, confirm the expected number of batches was retrieved and the reduced size of a batch (due to sampling):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L203-L205
```

:::info
Currently, the configuration of `Splitting` and `Sampling` as part of the `YAML` configuration is not supported; it must
be done using `batch_spec_passthrough` as illustrated above.
:::

## Additional Notes

Available `Splitting` methods and their configuration parameters:

    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | **Method**                      	| **Parameters**                                                                                                                        | **Returned Batch Data**                                                                                                                                                                   |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _split_on_whole_table             | N/A                                                                                                                                   | identical to original                                                                                                                                                                    	|
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _split_on_column_value            | column_name='col', batch_identifiers={ 'col': value }                                                                                 | rows where value of column_name are equal to value specified                                                                                                                             	|
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _split_on_converted_datetime      | column_name='col', date_format_string=<'%Y-%m-%d'>, batch_identifiers={ 'col': matching_string }                                      | rows where value of column_name converted to datetime using the given date_format_string are equal to matching string provided for the column_name specified                             	|
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _split_on_divided_integer         | column_name='col', divisor=<int>, batch_identifiers={ 'col': matching_divisor }                                                       | rows where value of column_name divided (using integral division) by the given divisor are equal to matching_divisor provided for the column_name specified                              	|
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _split_on_mod_integer             | column_name='col', mod=<int>, batch_identifiers={ 'col': matching_mod_value }                                                         | rows where value of column_name divided (using modular division) by the given mod are equal to matching_mod_value provided for the column_name specified                                 	|
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _split_on_multi_column_values     | column_names='<list[col]>', batch_identifiers={ 'col_0': value_0, 'col_1': value_1, 'col_2': value_2, ... }                           | rows where values of column_names are equal to values corresponding to each column name as specified                                                                                     	|
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _split_on_hashed_column           | column_name='col', hash_digits=<int>, hash_function_name=<'md5'> batch_identifiers={ 'hash_value': value }                            | rows where value of column_name hashed (using specified has_function_name) and retaining the stated number of hash_digits are equal to hash_value provided for the column_name specified 	|
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+


Available `Sampling` methods and their configuration parameters:

    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | **Method**                      	| **Parameters**                                                                                                                        | **Returned Batch Data**                                                                                                                                                                   |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _sample_using_random              | p=fraction                                                                                                                            | rows selected at random, whose number amounts to selected fraction of total number of rows in batch                                                                                       |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _sample_using_mod                 | column_name='col', mod=<int>                                                                                                          | take the mod of named column, and only keep rows that match the given value                                                                                                               |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _sample_using_a_list              | column_name='col', value_list=<list[val]>                                                                                             | match the values in the named column against value_list, and only keep the matches                                                                                                        |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _sample_using_hash                | column_name='col', hash_digits=<int>, hash_value=<str>, hash_function_name=<'md5'>                                                    | hash the values in the named column (using specified has_function_name), and only keep rows that match the given hash_value                                                               |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+


To view the full script used in this page, see it on GitHub:

- [yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py)
