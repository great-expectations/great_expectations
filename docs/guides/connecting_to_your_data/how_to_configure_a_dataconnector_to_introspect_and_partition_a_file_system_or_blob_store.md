---
title: How to introspect and partition a files based data store
---
import WhereToRunCode from './components/where_to_run_code.md'
import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx'

This guide will help you introspect and partition any files type data store (e.g., filesystem, cloud blob storage) using
the different types of the active `Data Connector`.  For background, please see the `Datasource` specific guides in the
"Connecting to your data" section.

The file based data introspection and partitioning mechanisms in Great Expectations are useful for:
- Exploring the types, subdirectory location, and filepath naming structures of the files in your dataset, and
- Organizing the discovered files into data assets according to the identified structures.

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

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L68-L77
```

Then load data into the `Validator` and print a brief excerpt of the file's contents (`n_rows = 5` is the default):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L83-L86
```

At this point, you can also perform additional checks, such as confirm the number of batches and the size of a batch.
For example (be sure to adjust this code to match the specifics of your data and configuration):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L88-L90
```

## Partitioning

Now use the `Configured Asset Data Connector` to gradually apply structure to the discovered assets and partition them
according to this structure.

### 1. Partition only by file name and type
 
You can employ a data asset that reflects a relatively general file structure (e.g., `taxi_data_flat` in the example
configuration) to represent files in a directory, which contain a certain prefix (e.g., `yellow_trip_data_sample_`) and
whose contents are of the desired type (e.g., CSV).

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L93-L97
```

For the present example, set `data_asset_name` to `taxi_data_flat` in the above `BatchRequest` specification.
(Customize for your own data set, as appropriate.)

Perform the relevant checks, such as confirm the number of batches and the size of a batch.
For example (be sure to adjust this code to match the specifics of your data and configuration):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L103-L105
```

### 2. Partition by year and month

Next, in recognition of a finer observed file path structure, you can refine the partitioning strategy.  For instance,
the `taxi_data_year_month` in our example configuration identifies three parts of a file path: `name` (as in "company
name"), `year`, and `month`.  This partitioning affords a rich set of filtering capabilities randing from specifying the
exact values of the file name structure's components to allowing Python functions for implementing custom criteria.

To illustrate (using the present configuration example), set `data_asset_name` to `taxi_data_year_month` in the
following `BatchRequest` specification (customize for your own data set, as appropriate):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L109-L114
```

To obtain the data for the nine months of February through October, apply the following custom filter:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L119-L122
```

Now, perform the relevant checks, such as confirm the expected number of batches was retrieved, and the size of a batch:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L124-L126
```

You can then identify a particular batch (e.g., corresponding to the year and month of interest) and retrieve it for
data analysis as follows:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L130-L141
```

Note that in the present example, there can be up to three `BATCH_FILTER_PARAMETER` key-value pairs, because the regular
expression for the data asset `taxi_data_year_month` defines three groups: `name`, `year`, and `month`.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L146-L149
```

(Be sure to adjust the above code snippets to match the specifics of your data and configuration.)

Now, perform the relevant checks, such as confirm the expected number of batches was retrieved, and the size of a batch:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L151-L153
```

### 2. Splitting and Sampling

Additional `Partitioning` mechanisms provided by Great Expectations include `Splitting` and `Sampling`.

`Splitting` provides the means of focusing the batch data on the values of certain dimensions of the data of interest.
To configure `Splitting`, specify a dimension (i.e., `column_name` or `column_names`), the method of splitting, and
parameters to be used by the specified splitting method.

`Sampling`, in turn, provides a means for reducing the amount of data in the retrieved batch to facilitate data analysis.
To configure `Sampling`, specify a dimension (i.e., `column_name` or the entire `table`), the method of sampling, and
parameters to be used by the specified sampling method.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L159-L187
```
```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L192-L195
```

For the present example, we can split according to the "passenger_count" column with the focus on two-passenger rides:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L196-L200
```

We can then obtain a random `10%` of the rows in the batch:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L201-L202
```

Finally, confirm the expected number of batches was retrieved and the reduced size of a batch (due to sampling):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L204-L206
```

:::info
Currently, the configuration of `Splitting` and `Sampling` as part of the `YAML` configuration is not supported; it must
be done using `batch_spec_passthrough` as illustrated above.
:::

## Additional Notes

To view the full scripts used in this page, see them on GitHub:

- [yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py)

# ALEX


### 2.  Examine a few rows of a file

Pick a `data_asset_name` from the previous step and specify it in the `BatchRequest`:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L68-L77
```

Then load data into the `Validator` and print a brief excerpt of the file's contents (`n_rows = 5` is the default):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L83-L86
```

At this point, you can also perform additional checks, such as confirm the number of batches and the size of a batch.
For example (be sure to adjust this code to match the specifics of your data and configuration):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L88-L90
```


#ALEX


Add the name of the data asset to the `data_asset_name` in your `BatchRequest`.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L62-L66
```
Then load data into the `Validator`.
```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L72-L78
```

#ALEX

1. **Construct a BatchRequest**

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L39-L44
    ```
   
    Since a `BatchRequest` can return multiple `Batch(es)`, you can optionally provide additional parameters to filter the retrieved `Batch(es)`. See [Datasources Core Concepts Guide](../../reference/datasources.md) for more info on filtering besides `batch_filter_parameters` and `limit` including custom filter functions and sampling. The example `BatchRequest`s below shows several non-exhaustive possibilities. 

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L61-L71
    ```

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L75-L89
    ```
   
    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L94-L104
    ```
   
    You may also wish to list available batches to verify that your `BatchRequest` is retrieving the correct `Batch(es)`, or to see which are available. You can use `context.get_batch_list()` for this purpose, which can take a variety of flexible input types similar to a `BatchRequest`. Some examples are shown below:

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L109-L114
    ```
    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L117-L118
    ```
   
    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L121-L127
    ```
   
    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L136-L142
    ```

2. **Get access to your Batch via a Validator**

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L147-L154
    ```

3. **Check your data**

    You can check that the first few lines of the `Batch` you loaded into your `Validator` are what you expect by running:

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L156
    ```

    Now that you have a `Validator`, you can use it to create `Expectations` or validate the data.
