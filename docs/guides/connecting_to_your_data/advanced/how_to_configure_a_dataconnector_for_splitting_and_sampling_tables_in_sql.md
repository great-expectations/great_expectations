---
title: How to configure a DataConnector for splitting and sampling tables in SQL
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'

This guide will help you configure `Splitting` and `Sampling` for working with tables in an SQL database using
`SimpleSqlalchemyDatasource`, which operates as a proxy to `InferredAssetSqlDataConnector` and
`ConfiguredAssetSqlDataConnector`.

We will use the `tables` section of the `SimpleSqlalchemyDatasource` configuration, which exercises the
`ConfiguredAssetSqlDataConnector`, to showcase `Splitting` and `Sampling` (the same `Splitting` and `Sampling`
configuration options can be readily applied to the `introspection` section of the `SimpleSqlalchemyDatasource`
configuration, which exercises the `InferredAssetSqlDataConnector`).

The `Splitting` and `Sampling` mechanisms provided by Great Expectations serve as additional tools for `Partitioning`
your data at various levels of granularity:
- `Splitting` provides the means of focusing the batch data on the values of certain dimensions of the data of interest.
- `Sampling` provides a means for reducing the amount of data in the retrieved batch to facilitate data analysis.

<Prerequisites>

- [Configured and loaded a Data Context](../../../tutorials/getting_started/initialize_a_data_context.md)
- [Configured a Datasource and Data Connector](../../../reference/datasources.md)
- Reviewed [How to configure a DataConnector to introspect and partition tables in SQL](../how_to_configure_a_dataconnector_to_introspect_and_partition_tables_in_sql.md)

</Prerequisites>

This guide will use the `tables` section that is part of the following `SimpleSqlalchemyDatasource` configuration as an
example:

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_complete.py#L9-L71
```

## Preliminary Steps

### 1. Instantiate your project's DataContext

Import these necessary packages and modules.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_complete.py#L3
```

Load your `DataContext` into memory using the `get_context()` method.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_complete.py#L7
```

### 2. Configure your Datasource

Using the above example configuration, specify the connection string for your database.  Then run this code to test your
configuration:

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_complete.py#L80
```

Feel free to adjust your configuration and re-run `test_yaml_config()` as needed.

### 3. Save the Datasource configuration to your DataContext

Save the configuration into your `DataContext` by using the `add_datasource()` function.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_complete.py#L82
```

## Splitting and Sampling

To configure `Splitting`, specify a dimension (i.e., `column_name` or `column_names`), the method of `Splitting`, and
parameters to be used by the specified `Splitting` method.  In the present example, the `Data Connectors` named
`by_num_riders` and `by_num_riders_random_sample` split the table `yellow_tripdata_sample_2019_01` on the column name
`passenger_count` using the `_split_on_column_value` method of `Splitting`.

To configure `Sampling`, specify the method of `Sampling`, and parameters to be used by the specified `Sampling` method.
In the present example, the `Data Connector` named `by_num_riders_random_sample` samples the table
`yellow_tripdata_sample_2019_01` using the `_sample_using_random` method of `Sampling`, configured to return `10%` of
the rows sampled at random, which is specified by the parameter `p` (stands for "proportion") set to the value `0.1`.

Finally, confirm the expected number of batches was retrieved and the reduced size of a batch (due to `Sampling`):

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_complete.py#L164-L168
```

(set `data_asset_name` to `"taxi__yellow_tripdata_sample_2019_01__asset"` for the present example).

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_complete.py#L174-L181
```

## Additional Notes

Available `Splitting` methods and their configuration parameters:

    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+-----------------+
    | **Method**                      	| **Parameters**                                                                                                                        | **Returned Batch Data**                                                                                              |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+-----------------+
    | _split_on_whole_table             | N/A                                                                                                                                   | identical to original                                                                                                |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+-----------------+
    | _split_on_column_value            | table_name='table', column_name='col'                                                                                                 | rows where value of column_name are same                                                                             |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+-----------------+
    | _split_on_converted_datetime      | table_name='table', column_name='col', date_format_string=<'%Y-%m-%d'>                                                                | rows where value of column_name converted to datetime using the given date_format_string are same                    |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+-----------------+
    | _split_on_divided_integer         | table_name='table', column_name='col', divisor=<int>                                                                                  | rows where value of column_name divided (using integral division) by the given divisor are same                      |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+-----------------+
    | _split_on_mod_integer             | table_name='table', column_name='col', mod=<int>                                                                                      | rows where value of column_name divided (using modular division) by the given mod are same                           |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+-----------------+
    | _split_on_multi_column_values     | table_name='table', column_names='<list[col]>'                                                                                        | rows where values of column_names are same                                                                           |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+-----------------+
    | _split_on_hashed_column           | column_name='col',                                                                                                                    | rows where value of column_name hashed (using "md5" hash function) are same (experimental)                           |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------+


Available `Sampling` methods and their configuration parameters:

    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------+
    | **Method**                      	| **Parameters**                                                                                                                        | **Returned Batch Data**                                                                                              |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------+
    | _sample_using_limit               | n=num_rows                                                                                                                            | first up to to n (specific limit parameter) rows of batch                                                            | 
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------+
    | _sample_using_random              | p=fraction                                                                                                                            | rows selected at random, whose number amounts to selected fraction of total number of rows in batch                  |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------+
    | _sample_using_mod                 | column_name='col', mod=<int>                                                                                                          | take the mod of named column, and only keep rows that match the given value                                          |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------+
    | _sample_using_a_list              | column_name='col', value_list=<list[val]>                                                                                             | match the values in the named column against value_list, and only keep the matches                                   |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------+
    | _sample_using_hash                | column_name='col', hash_digits=<int>, hash_value=<str>                                                                                | hash the values in the named column (using "md5" hash function), and only keep rows that match the given hash_value  |
    +-----------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------+


To view the full script used in this page, see it on GitHub:

- [yaml_example_complete.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_complete.py)
