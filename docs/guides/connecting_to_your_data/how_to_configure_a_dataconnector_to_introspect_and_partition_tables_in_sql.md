---
title: How to configure a DataConnector to introspect and partition tables in SQL
---
import WhereToRunCode from './components/where_to_run_code.md'
import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx'

This guide will help you introspect and tables in an SQL database using `SimpleSqlalchemyDatasource`, which operates as
a proxy to `InferredAssetSqlDataConnector` and `ConfiguredAssetSqlDataConnector` (for the SQL use case,
these types of the active `Data Connector` are exercised indirectly, via `SimpleSqlalchemyDatasource`).  For background,
please see the `Datasource` specific guides in the "Connecting to your data" section.

The SQL database introspection and partitioning mechanisms in Great Expectations are useful for:
- Exploring the schema and column metadata of the tables in your SQL database, and
- Organizing the tables into data assets according to the partitioning considerations informed by this exploration.

Partitioning enables you to select the desired subsets of your dataset for [Validation](/docs/reference/validation).

<Prerequisites>

- [Configured and loaded a Data Context](../../tutorials/getting_started/initialize_a_data_context.md)
- [Configured a Datasource and Data Connector](../../reference/datasources.md)
  
</Prerequisites>

This guide will use the following `Datasource` configuration as an example:
```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example.py#L9-L66
```

To learn more about `Datasources`, `Data Connectors`, and `Batch(es)`, please see our [Datasources Core Concepts Guide](../../reference/datasources.md) in the [Core Concepts reference guide](../../reference/core_concepts.md). 

## Preliminary Steps

### 1. Choose how to run the code in this guide

<WhereToRunCode />

### 2. Instantiate your project's DataContext

Import these necessary packages and modules.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example.py#L1-L5
```

Load your DataContext into memory using the `get_context()` method.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example.py#L7
```

### 3. Configure your Datasource

Using the above example configuration, specify the connection string for your database.  Then run this code to test your
configuration:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example.py#L75
```

You will then see your tables listed as `Available data_asset_names` in the output of `test_yaml_config()`.

Feel free to adjust your configuration and re-run `test_yaml_config()` as needed.

### 4. Save the Datasource configuration to your DataContext

Save the configuration into your `DataContext` by using the `add_datasource()` function.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example.py#L77
```

## Introspection

### 1. Get names of available Data Assets
 
Getting names of available data assets using the data connector keys referenced in the `introspection` section of the
configuration gives you visibility into the tables in your database:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example.py#L78-L80
```

### 2.  Examine a few rows of a table

Pick a `data_asset_name` from the previous step and specify it in the `BatchRequest`:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example.py#L84-L88
```

Then load data into the `Validator` and print a few rows of the table (`n_rows = 5` is the default):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example.py#L94-L99
```

At this point, you can also perform additional checks, such as confirm the number of batches and the size of a batch.
For example (be sure to adjust this code to match the specifics of your data and configuration):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example.py#L102-L108
```

### 3. Using Splitting to aid introspection

`Splitting` is a `Data Connector` capability that provides the means of breaking the batch data up based on the values
of certain dimensions of the data of interest.  Employing `Splitting` as part of introspection can help focus on smaller
batches, centered around a common aspect of the data.

To configure `Splitting`, specify a dimension (i.e., `column_name` or `column_names`), the method of splitting, and
parameters to be used by the specified splitting method.

For the present example, we can split according to the "pickup_datetime" column parsed to the date level precision and
thus obtain batches containing the taxi rides data for each day found in the available data:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example.py#L112-L116
```

Perform the relevant checks, such as confirm the number of batches.  For example (be sure to adjust this code to match
the specifics of your data and configuration):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example.py#L122-L123
```

As an additional example, we can split according to the "pickup_datetime" column parsed to the hour level precision and
thus obtain batches containing the taxi rides data for all hours of each day found in the available data:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example.py#L127-L131
```

Perform the relevant checks, such as confirm the number of batches.  For example (be sure to adjust this code to match
the specifics of your data and configuration):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example.py#L137-L138
```

Note that in the introspection mode, all included tables will contribute rows to the composition of the batch data
representing the splits.

## Partitioning

In the partitioning mode, a dedicated data asset is assigned to a table, and `Splitting` is used to associate the
resulting batches with desired semantics.  Partitioning is fully specified in the `tables` section of the configuration.

For the present example, we can split according to the "passenger_count" column with the focus on two-passenger rides:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example.py#L142-L146
```

Perform the relevant checks, such as confirm the number of batches.  For example (be sure to adjust this code to match
the specifics of your data and configuration):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example.py#L152-L153
```

## Sampling

`Sampling` provides a means for reducing the amount of data in the retrieved batch to facilitate data analysis.

To configure `Sampling`, specify a dimension (i.e., `column_name` or the entire `table`), the method of sampling, and
parameters to be used by the specified sampling method.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example.py#L159-L163
```

We can then obtain a random `10%` of the rows in the batch:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example.py#L169
```

Finally, confirm the expected number of batches was retrieved and the reduced size of a batch (due to sampling):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example.py#L170-L176
```

## Additional Notes

Available `Splitting` methods and their configuration parameters:

    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | **Method**                      	| **Parameters**                                                                                                                        | **Returned Batch Data**                                                                                                                                                                   |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _split_on_whole_table             | N/A                                                                                                                                   | identical to original                                                                                                                                                                    	|
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _split_on_column_value            | column_name='col'                                                                                                                     | rows where value of column_name are same                                                                                                                                              	|
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _split_on_converted_datetime      | column_name='col', date_format_string=<'%Y-%m-%d'>                                                                                    | rows where value of column_name converted to datetime using the given date_format_string are same                                                                                       	|
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _split_on_divided_integer         | column_name='col', divisor=<int>                                                                                                      | rows where value of column_name divided (using integral division) by the given divisor are same                                                                                           |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _split_on_mod_integer             | column_name='col', mod=<int>                                                                                                          | rows where value of column_name divided (using modular division) by the given mod are same                                                                                                |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _split_on_multi_column_values     | column_names='<list[col]>', batch_identifiers={    'col_0': value_0,    'col_1': value_1,    'col_2': value_2,               ... }    | rows where values of column_names are same                                                                                                                                              	|
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _split_on_hashed_column           | column_name='col',                                                                                                                    | rows where value of column_name hashed (using "md5" hash function) are same (experimental)                                                                                                |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+


Available `Sampling` methods and their configuration parameters:

    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | **Method**                      	| **Parameters**                                                                                                                        | **Returned Batch Data**                                                                                                                                                                   |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _sample_using_limit               | n=num_rows                                                                                                                            | first up to to n (specific limit parameter) rows of batch                                                                                                                                 | 
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _sample_using_random              | p=fraction                                                                                                                            | rows selected at random, whose number amounts to selected fraction of total number of rows in batch                                                                                       |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _sample_using_mod                 | column_name='col', mod=<int>                                                                                                          | take the mod of named column, and only keep rows that match the given value                                                                                                               |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _sample_using_a_list              | column_name='col', value_list=<list[val]>                                                                                             | match the values in the named column against value_list, and only keep the matches                                                                                                        |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+
    | _sample_using_hash                | column_name='col', hash_digits=<int>, hash_value=<str>                                                                                | hash the values in the named column (using "md5" hash function), and only keep rows that match the given hash_value                                                                       |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	+


To view the full script used in this page, see it on GitHub:

- [yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example.py)
