---
id: migration_guide
title: "GX V0 to V1 Migration Guide"
---

## Overview
This guide for migrating your Great Expectations V0 configurations to V1 covers all the Great Expectations domain objects found in V0 and shows how they map to their equivalent V1 domain objects.

### GX Cloud Context Users
If you are a GX cloud user, you are able to immediately try out GX V1! Cloud will do the translation of your configurations for you. Your `context = gx.get_context()` call will return updated configurations. You can inspect your configuration objects by calling `all()` on the appropriate domain namespace. For example, `context.data_sources.all()` will list all of your datasources that have been automatically translated to V1. If there are incompatible configurations, they will be filtered out of this list. You can retrieve them by using a GX `>=0.18.19` Python client. If you need to translate any of these missing configurations to `1.0` you can look at the various **API** sections below the domain object you are interested in to see a comparison of the V0 and V1 API calls and determine what you need to do to translate the configuration.

### GX File Context 
Below in each section you will see a side-by-side comparison of the configuration files for each domain object along with a description of how they have changed and what features have been removed and added. You can use this as a basis for translating your configuration objects from V0 to V1.

## Domain objects

### Expectation Suites and Expectations
In GX `0.X` and in GX `1.0`, every Expectation Suite has its own configuration file and the path to them in the Great Expectations project directory is:

`gx/expectations/<suite_name>.json`

#### Configuration file differences

Here is a side-by-side comparison of a suite called `suite_for_yellow_tripdata`:

<table>
    <tr>
        <th>V0 Expectation Suite Configuration</th>
        <th>V1 Expectation Suite Configuration</th>
    </tr>
    <tr>
        <td><pre>
        ```json
            {
                "expectation_suite_name": "suite_for_yellow_tripdata",
                "data_asset_type": "CSVAsset",
                "evaluation_parameters": {
                    "parameter_name": "value"
            },
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_be_between",
                    "kwargs": {
                        "column": "passenger_count",
                        "max_value": 4,
                        "min_value": 0
                    },
                    "meta": {}
                },
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {
                        "column": "VendorID",
                        "value_set": [
                            1,
                            2,
                            3,
                            4
                        ]
                    },
                "meta": {}
                }
            ],
            "ge_cloud_id": null,
            "meta": {
                "foo": "bar",
                "great_expectations_version": "0.18.19"
                }
            }              
        ```
        </pre></td>
        <td><pre>
        ```json
        {
            "name": "suite_for_yellow_tripdata",
            "suite_parameters": {
                "parameter_name": "value"
            },
            "expectations": [
                {
                    "type": "expect_column_values_to_be_between",
                    "kwargs": {
                        "column": "passenger_count",
                        "max_value": 4.0,
                        "min_value": 0.0
                    },
                    "meta": {},
                    "id": "24dc475c-38a3-4234-ab47-b13d0f233242"
                },
                {
                    "type": "expect_column_values_to_be_in_set",
                    "kwargs": {
                        "column": "VendorID",
                        "value_set": [
                            1,
                            2,
                            3,
                            4
                        ]
                    },
                    "meta": {},
                    "id": "d8b3b4e9-296f-4dd5-bd29-aac6a00cba1c"
                }
            ],
            "id": "77373d6f-3561-4d62-b150-96c36dccbe55",
            "meta": {
                "foo": "bar",
                "great_expectations_version": "1.0.0"
            },
            "notes": "This is a new field."
        }            
        ```
        </pre></td>
    </tr>
</table>

**expectation_suite_name**: This is now called name and has the name of the suite.

**data_asset_type**: This has been removed. Expectation suites can be associated with any asset type.

**evaluation_parameters**: This has been renamed to suite_parameters. The contents are unchanged.

**expectations**: This is a list of expectations. The expectation keys have changed as follows

> **expectation_type**: This has been changed to type.

> **kwargs**: This is unchanged

> **meta**: This dictionary that a user can populate with whatever metadata they would like. The notes key that Great Expectations Cloud used has been pulled out into a top level key.

> **id**: This new field introduced in 1.0 can be any arbitrary, unique UUID. When migrating, generate and add a UUID.

> **notes (new field)**: This new top-level field replaces meta.notes. This is consumed by Great Expectations Cloud to display user notes on the Cloud UI.

**ge_cloud_id**: This is now id. This is now a required field. Migrators can generate a unique, arbitrary UUID and add it.

**meta**: The format is unchanged.

**notes**: This is new in 1.0 and is an arbitrary string.

#### Suite Creation API Calls

The suites above were created with the following API calls. This example demonstrates how to create an equivalent suite to your V0 suite in V1.

<table>
    <tr>
        <th>V0 Expectation Suite API</th>
        <th>V1 Expectation Suite API</th>
    </tr>
    <tr>
        <td>
        ```python
        suite = context.add_expectation_suite(
            expectation_suite_name="suite_for_yellow_tripdata",
            meta={"foo": "bar", "notes": "Here are some suite notes."},
            evaluation_parameters={"parameter_name": "value"},
            data_asset_type="CSVAsset", # V1 no longer supports this argument, expectations are type independent
        )
        validator = context.get_validator(batch_request=asset.build_batch_request(), expectation_suite_name="suite_for_yellow_tripdata")
        validator.expect_column_values_to_be_between(column="passenger_count", min_value=0, max_value=4)
        validator.expect_column_values_to_be_in_set(column="VendorID", value_set=[1,2,3,4])
        validator.save_expectation_suite(discard_failed_expectations=False)
        ```
        </td>
        <td>
        ```python
        suite = context.suites.add(
            gx.ExpectationSuite(
                name="suite_for_yellow_tripdata",
                meta={"foo": "bar"},
                suite_parameters={"parameter_name": "value"},
                notes="Here are some suite notes.",
                id="77373d6f-3561-4d62-b150-96c36dccbe55",
            )
        )
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="passenger_count", min_value=0, max_value=4))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="VendorID", value_set=[1,2,3,4]))
        ```
        </td>
    </tr>
</table>

### Data Sources and Data Assets
Data Source configurations are stored in the YAML file `gx/great_expectations.yml`, in the top-level block whose key is `fluent_datasources.` 

We’ll walk through examples of different Data Source configurations in V0 and V1 so you can see how to translate between the two.

#### Pandas

##### Pandas Filesystem Data
Here is a side-by-side comparison of a Data Source called `pandas_fs_ds` with 4 assets called: `yearly_taxi_data`, `monthly_taxi_data`, `daily_taxi_data`, and `arbitrary_taxi_data`.

<table>
    <tr>
        <th>V0 Pandas Filesystem Config</th>
        <th>V1 Pandas Filesystem Config</th>
    </tr>
    <tr>
        <td>
        ```yaml
        fluent_datasources:
            pandas_fs_ds:
                type: pandas_filesystem
                assets:
                yearly_taxi_data:
                    type: csv
                    batching_regex: sampled_yellow_tripdata_(?P<year>\d{4})\.csv
                monthly_taxi_data:
                    type: csv
                    batching_regex: sampled_yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})\.csv
                daily_taxi_data:
                    type: csv
                    batching_regex: sampled_yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\.csv
                arbitrary_taxi_data:
                    type: csv
                    batching_regex: sampled_yellow_tripdata_(?P<code>\w+)\.csv
                base_directory: data
        ```
        </td>
        <td>
        ```yaml
        fluent_datasources:
            pandas_fs_ds:
                type: pandas_filesystem
                id: 2ea309bf-bb5f-421b-ab6b-ea1cc9e70c8e
                assets:
                taxi_data:
                    type: csv
                    id: 34b98eca-790f-4504-ab4b-b65bc128b5ee
                    batch_definitions:
                    yearly_batches:
                        id: a04f8071-33d9-4834-b667-e3d8c2ca70aa
                        partitioner:
                        regex: sampled_yellow_tripdata_(?P<year>\d{4})\.csv
                        sort_ascending: true
                    monthly_batches:
                        id: f07aa73d-bf56-438e-9dc2-0d05fb7d32a1
                        partitioner:
                        regex: sampled_yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})\.csv
                        sort_ascending: true
                        param_names:
                            - year
                            - month
                    daily_batches:
                        id: 37b4b2eb-4b37-46c6-b51c-f2d21ba0e6d6
                        partitioner:
                        regex: sampled_yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\.csv
                        sort_ascending: true
                        param_names:
                            - year
                            - month
                            - day
                base_directory: data
        ```
        </td>
    </tr>
</table>

In `0.X`, a Data Source represents where the data lives and the execution engine (e.g. reading data from the local filesystem using pandas) and a Data Asset represents the data file format and how the data should be partitioned (e.g. a parameterized regex which matches file names). In `1.0` the Data Source has the same meaning. However, the Data Asset now only represents the data file format and there is a new concept, *Batch Definition*, which represents how the data is partitioned. This manifests as an extra layer in the YAML asset block.

**pandas_fs_ds (example)**: The keys below `fluent_datasources` are the names of the Data Source. This is unchanged.

**type**: The type of Data Source. This is unchanged.

**assets**: A list of the Data Assets. Each key is an asset name in both V0 and V1. The asset value is different. In V0 the nested keys are:

> **type**: This is unchanged

> **batching_regex**: This has been replaced with batch_definitions. The format for batch_definitions follows. You will notice that the regex now lives in the partitioners regex field. The batch_definition configuration format is:

>> **yearly_batches (example Batch Definition name)**: These keys are the names of the batch definitions.

>> **id**: This is an arbitrary UUID and can be chosen to be any unique UUID.

>> **partitioner**: This is a key with information about how the batch is defined

>>> **regex**: This is the regex previously living on the asset keyed by batching_regex

>>> **sort_ascending**: A boolean. `true` if the batch order is increasing in time, `false` if the ordering is decreasing in time. Previously in V0 one could specify an order_by field on the asset which could sort the different date components in different orders (eg year could be sorted increasing in time while month could be sorted decreasing in time). This is no longer supported.

>>> **param_names**: This is a list of the parameter names which will be identical to the named matches from the `regex`. That is, the items will be `year`, `month`, or `day`. If this list would only contain year it can be excluded from the configuration file.

> **id**: This is a new field and is an arbitrary UUID. If migrating you can pick any unique UUID.

**base_directory**: The path to the data files. This is unchanged.

**id**: This is a new field and is an arbitrary UUID. If migrating you can pick any unique UUID.

:::note
We no longer support arbitrary batching regexes. Batches must be defined by one of our temporal batch definitions which are yearly, monthly, or daily.
:::

##### Pandas Filesystem Creation via API

<table>
    <tr>
        <th>V0 Pandas Filesystem Creation via API</th>
        <th>V1 Pandas Filesystem Creation via API</th>
    </tr>
    <tr>
        <td>
        ```python
        # Pandas Filesystem Data Source
        datasource = context.sources.add_pandas_filesystem(name="pd_fs_ds", base_directory="data")

        # Data Assets
        yearly = datasource.add_csv_asset(name="yearly_taxi_data", batching_regex=r"sampled_yellow_tripdata_(?P<year>\d{4})\.csv")
        monthly = datasource.add_csv_asset(name="monthly_taxi_data", batching_regex=r"sampled_yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})\.csv")
        daily = datasource.add_csv_asset(name="daily_taxi_data", batching_regex=r"sampled_yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\.csv")
        arbitrary = datasource.add_csv_asset(name="arbitrary_taxi_data", batching_regex=r"sampled_yellow_tripdata_(?P<code>\w+)\.csv")
        ```
        </td>
        <td>
        ```python
        # Pandas Filesystem Data Source
        data_source = context.data_sources.add_pandas_filesystem(name="pd_fs_ds", base_directory="data")

        # CSV Data Asset
        file_csv_asset = data_source.add_csv_asset(name="taxi_data")

        # Batch Definitions
        yearly = file_csv_asset.add_batch_definition_yearly(name="yearly_batches", regex=r"sampled_yellow_tripdata_(?P<year>\d{4})\.csv")
        monthly = file_csv_asset.add_batch_definition_monthly(name="monthly_batches", regex=r"sampled_yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})\.csv")
        daily = file_csv_asset.add_batch_definition_daily(name="daily_batches", regex=r"sampled_yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\.csv")
        ```
        </td>
    </tr>
</table>

##### Pandas Dataframe
<table>
    <tr>
        <th>V0 Pandas Dataframe Config</th>
        <th>V1 Pandas Dataframe Config</th>
    </tr>
    <tr>
        <td>
        ```yaml
        fluent_datasources:
        pd_df_ds:
            type: pandas
            assets:
            taxi_dataframe_asset:
                type: dataframe
                batch_metadata: {}
        ```
        </td>
        <td>
        ```yaml
        fluent_datasources:
            pd_df_ds:
                type: pandas
                assets:
                taxi_dataframe_asset:
                    type: dataframe
                    batch_metadata: {}
                    batch_definitions:
                    taxi_dataframe_batch_def:
                        id: bf0de640-7791-4654-86b0-5f737319e993
                        partitioner:
                    id: 352b392d-f0a5-4c7c-911f-fd68903599e0
                id: 4e0a4b9c-efc2-40e8-8114-6a45ac697554
        ```
        </td>
    </tr>
</table>

In both `V0` and `V1` a pandas Data Source reads in data from a pandas dataframe. In `V1` there is a concept of a *Batch Definition* that is used to partition data into batches. For a pandas dataframe the only *Batch Definition* currently available is the whole dataframe *Batch Definition*.

**pd_df_ds (example)**: The keys below fluent_datasources are the names of the Data Sources. This is unchanged.

**assets**: A list of the Data Assets. Each key is an asset name in both V0 and V1. The asset value is different.

> **type**: The type of Data Source. This is unchanged.

> **batch_metadata**: Arbitrary key/values pairs used to annotate the Data Asset. In V1 this is unchanged, it still describes the asset.

> **batch_definitions**: This is new in V1. There is only 1 option here. The key is the name of the Batch Definition. It has 2 fields:

>> **id**: An arbitrary UUID. Migrators can assign any unique UUID.

>> **partitioner**: This is left empty as we only allow the whole dataframe

> **id**: In V1, the asset has a unique ID. Migrators can assign any unique UUID.

**id**: In V1, the Data Source has a unique ID. Migrators can assign any unique UUID.

##### Pandas Dataframe Creation via API 
<table>
    <tr>
        <th>V0 Pandas Dataframe Creation via API</th>
        <th>V1 Pandas Dataframe Creation via API</th>
    </tr>
    <tr>
        <td>
        ```python
        dataframe_ds = context.sources.add_pandas(name="pd_df_ds")
        dataframe_asset = dataframe_ds.add_dataframe_asset(name="taxi_dataframe_asset")
        ```
        </td>
        <td>
        ```python
        dataframe_ds = context.data_sources.add_pandas(name="pd_df_ds")
        dataframe_asset = dataframe_ds.add_dataframe_asset(name="taxi_dataframe_asset")
        dataframe_bd = dataframe_asset.add_batch_definition_whole_dataframe(name="taxi_dataframe_batch_def")
        ```
        </td>
    </tr>
</table>

#### Snowflake
Here is a side-by-side comparision of a both a `V0` Snowflake table and query Data Asset to their equivalents in `V1`. We walk through all the currently supported V1 Batch Definitions: yearly, monthly, daily, and whole table.

<table>
    <tr>
        <th>V0 Snowflake Config</th>
        <th>V1 Snowflake Config</th>
    </tr>
    <tr>
        <td>
        ```yaml
        fluent_datasources:
            snowflake_ds:
                type: snowflake
                assets:
                yearly_taxi_data:
                    type: table
                    order_by:
                    - key: year
                        reverse: false
                    batch_metadata: {}
                    splitter:
                    column_name: pickup_datetime
                    method_name: split_on_year
                    table_name: TAXI_DATA_ALL_SAMPLES
                    schema_name:
                monthly_taxi_data:
                    type: table
                    order_by:
                    - key: year
                        reverse: true
                    - key: month
                        reverse: true
                    batch_metadata: {}
                    splitter:
                    column_name: pickup_datetime
                    method_name: split_on_year_and_month
                    table_name: TAXI_DATA_ALL_SAMPLES
                    schema_name:
                daily_taxi_data:
                    type: table
                    order_by:
                    - key: year
                        reverse: false
                    - key: month
                        reverse: false
                    - key: day
                        reverse: false
                    batch_metadata: {}
                    splitter:
                    column_name: pickup_datetime
                    method_name: split_on_year_and_month_and_day
                    table_name: TAXI_DATA_ALL_SAMPLES
                    schema_name:
                all_taxi_data:
                    type: table
                    order_by: []
                    batch_metadata: {}
                    table_name: TAXI_DATA_ALL_SAMPLES
                    schema_name:
                query_yearly:
                    type: query
                    order_by:
                    - key: year
                        reverse: false
                    batch_metadata: {}
                    splitter:
                    column_name: pickup_datetime
                    method_name: split_on_year
                    query: select * from TAXI_DATA_ALL_SAMPLES
                query_monthly:
                    type: query
                    order_by:
                    - key: year
                        reverse: true
                    - key: month
                        reverse: true
                    batch_metadata: {}
                    splitter:
                    column_name: pickup_datetime
                    method_name: split_on_year_and_month
                    query: select * from TAXI_DATA_ALL_SAMPLES
                query_daily:
                    type: query
                    order_by:
                    - key: year
                        reverse: false
                    - key: month
                        reverse: false
                    - key: day
                        reverse: false
                    batch_metadata: {}
                    splitter:
                    column_name: pickup_datetime
                    method_name: split_on_year_and_month_and_day
                    query: select * from TAXI_DATA_ALL_SAMPLES
                whole_query:
                    type: query
                    order_by: []
                    batch_metadata: {}
                    query: select * from TAXI_DATA_ALL_SAMPLES
                connection_string: 
                snowflake://<user_login_name>:<password>@<account_identifier>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>    
        ```
        </td>
        <td>
        ```yaml
        fluent_datasources:
            snowflake_ds:
                type: snowflake
                id: f4ac98d6-dccf-4373-b5f3-ac90ed60b139
                assets:
                taxi_data:
                    type: table
                    id: ad9e8ece-0c14-45bc-bcdd-ef2e40922df4
                    batch_metadata: {}
                    batch_definitions:
                    table_yearly:
                        id: 75a41bce-da84-425f-a3d3-92acd5c5f7f8
                        partitioner:
                        column_name: PICKUP_DATETIME
                        sort_ascending: true
                        method_name: partition_on_year
                    table_monthly:
                        id: 67ec396a-e7ca-499d-8cb7-84a803d976af
                        partitioner:
                        column_name: PICKUP_DATETIME
                        sort_ascending: false
                        method_name: partition_on_year_and_month
                    table_daily:
                        id: 7d410bd4-ca6d-464d-b82d-3b070e6fd229
                        partitioner:
                        column_name: PICKUP_DATETIME
                        sort_ascending: true
                        method_name: partition_on_year_and_month_and_day
                    whole_table:
                        id: bd88cdd9-a5f4-4bdf-bbf3-e43827996dd0
                        partitioner:
                    table_name: TAXI_DATA_ALL_SAMPLES
                    schema_name: public
                query_data:
                    type: query
                    id: 44b0eccc-54f2-46e1-a6f9-3558662d4f8a
                    batch_metadata: {}
                    batch_definitions:
                    query_yearly:
                        id: 7f3909d4-912f-44aa-8140-7ab4e7b13f4e
                        partitioner:
                        column_name: PICKUP_DATETIME
                        sort_ascending: true
                        method_name: partition_on_year
                    query_monthly:
                        id: d0c347fc-03e5-4880-a8e8-1eff04432c2f
                        partitioner:
                        column_name: PICKUP_DATETIME
                        sort_ascending: false
                        method_name: partition_on_year_and_month
                    query_daily:
                        id: 1f6701bd-b470-4ddb-a001-4cc6167ab4d0
                        partitioner:
                        column_name: PICKUP_DATETIME
                        sort_ascending: true
                        method_name: partition_on_year_and_month_and_day
                    whole_query:
                        id: 4817cf80-1727-4aad-b31a-5552efeea441
                        partitioner:
                    query: SELECT * FROM TAXI_DATA_ALL_SAMPLES
                connection_string: 
                    snowflake://<user_login_name>:<password>@<account_identifier>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>
        ```
        </td>
    </tr>
</table>

In `0.X`, a Data Source represents where the data is persisted and the execution engine (e.g. the Snowflake database) and a Data Asset represents the data and how the data should be partitioned (e.g. by a datetime column). In `1.0` the Data Source has the same meaning. However, the Data Asset now represents only the data and there is a new concept, the batch definition, which represents how the data is partitioned. This manifests as an extra layer in the YAML asset block.

A few configurations are **NO LONGER SUPPORTED**:

- In V1, we currently only allow batching by time (e.g. year, month, day). In V0 one could split the data into batches in lots of ways. For example a table could be split by a value in a column or a file regex could contain arbitrary match expressions. We consider non-time based splitting to represent different conceptual assets that may happen to reside in the same table. For those, one should compute views or use a query asset.

- In V0, one could set the sorting order independently for the year, month, and day dimensions. That is, one could sort ascending by year, but then descending by month and day. In V1 we only allow sorting of all the batches in ascending or descending order. For example, one can no longer sort year and month in the opposite order.

**snowflake_ds (example)**: The keys under fluent_datasources are the names of the datasources.

**type**: The type of Data Source, this is unchanged.

**assets**: The keys to Data Assets are the names of the assets. In this example yearly_taxi_data is the name of a V0 asset. In V1, the asset is called taxi_data.

> **type**: The type of asset (table or query). This is unchanged.

> **order_by**: This is no longer a key. The information has moved inside the V1 batch definitions under the partitioner.

> **splitter**: This is no longer a key has been replaced by batch_definitions. The format for batch definitions is:

>> **table_yearly (an example)**: The name of the Batch Definition is the key to each configuration.

>>> **id**:  Migrators can assign any unique UUID.

>>> **partitioner**: Contains the batching and sorting information. This has no value for a “whole table” partitioner.

>>>> **column_name**: The column on which to split the data. This must be a DATETIME field.

>>>> **sort_ascending**: A boolean. true sorts the most recent batch first, while false sorts with the most recent batch last.

>>>> **method_name**: A string indicating the batching resolution. The options are: partition_on_year, partition_on_year_and_month, partition_on_year_and_month_and_day.

> **batch_metadata**: This is unchanged.

> **table_name (TableAsset only)**: The name of the table that holds data for this asset. This is unchanged.

> **schema_name (TableAsset only)**: The name of the schema to which the table belongs. In V1 this is now a required field.

> **query (QueryAsset only)**: The query to be run to generate the data for this asset. This is unchanged.

> **id (New in V1)**: This is a new field in V1 and is a random UUID. Migrators can assign any unique UUID.

**id (New in V1)**: An id for the Data Asset. Migrators can assign any unique UUID.

##### Snowflake Creation via API
<table>
    <tr>
        <th>V0 Snowflake Creation via API</th>
        <th>V1 Snowflake Creation via API</th>
    </tr>
    <tr>
        <td>
        ```python
        # Create datasource
        connection_string = "snowflake://<user_login_name>:<password>@<account_identifier>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>"
        snowflake_ds = context.sources.add_snowflake(name="snowflake_ds", connection_string=connection_string)

        # Create table assets
        yearly_snowflake_asset = snowflake_ds.add_table_asset(name="yearly_taxi_data", table_name="TAXI_DATA_ALL_SAMPLES", order_by=["+year"])
        yearly_snowflake_asset.add_splitter_year(column_name="pickup_datetime")
        monthly_snowflake_asset = snowflake_ds.add_table_asset(name="monthly_taxi_data", table_name="TAXI_DATA_ALL_SAMPLES", order_by=["-year", "-month"])
        monthly_snowflake_asset.add_splitter_year_and_month(column_name="pickup_datetime")
        daily_snowflake_asset = snowflake_ds.add_table_asset(name="daily_taxi_data", table_name="TAXI_DATA_ALL_SAMPLES", order_by=["+year", "+month", "+day"])
        daily_snowflake_asset.add_splitter_year_and_month_and_day(column_name="pickup_datetime")
        whole_table_snowflake_asset = snowflake_ds.add_table_asset(name="all_taxi_data", table_name="TAXI_DATA_ALL_SAMPLES")

        # Create query assets
        yearly_query_asset = snowflake_ds.add_query_asset(name="query_yearly", query="select * from TAXI_DATA_ALL_SAMPLES", order_by=["+year"])
        yearly_query_asset.add_splitter_year(column_name="pickup_datetime")
        monthly_query_asset = snowflake_ds.add_query_asset(name="query_monthly", query="select * from TAXI_DATA_ALL_SAMPLES", order_by=["-year", "-month"])
        monthly_query_asset.add_splitter_year_and_month(column_name="pickup_datetime")
        daily_query_asset = snowflake_ds.add_query_asset(name="query_daily", query="select * from TAXI_DATA_ALL_SAMPLES", order_by=["+year", "+month", "+day"])
        daily_query_asset.add_splitter_year_and_month_and_day(column_name="pickup_datetime")
        query_whole_table_asset = snowflake_ds.add_query_asset(name="whole_query", query="select * from TAXI_DATA_ALL_SAMPLES")
        ```
        </td>
        <td>
        ```python
        # Create datasource
        connection_string = "snowflake://<user_login_name>:<password>@<account_identifier>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>"
        snowflake_ds = context.sources.add_snowflake(name="snowflake_ds", connection_string=connection_string)

        # Create table asset and batch definitions
        table_asset = snowflake_ds.add_table_asset(name="taxi_data", table_name="TAXI_DATA_ALL_SAMPLES")
        table_yearly = table_asset.add_batch_definition_yearly(name="table_yearly", column="PICKUP_DATETIME", sort_ascending=True)
        table_monthly = table_asset.add_batch_definition_monthly(name="table_monthly", column="PICKUP_DATETIME", sort_ascending=False)
        table_daily = table_asset.add_batch_definition_daily(name="table_daily", column="PICKUP_DATETIME", sort_ascending=True)
        whole_table = table_asset.add_batch_definition_whole_table(name="whole_table")

        # Create query asset and batch definitions
        query_asset = snowflake_ds.add_query_asset(name="query_data", query="SELECT * FROM TAXI_DATA_ALL_SAMPLES")
        query_yearly = query_asset.add_batch_definition_yearly(name="query_yearly", column="PICKUP_DATETIME", sort_ascending=True)
        query_monthly = query_asset.add_batch_definition_monthly(name="query_monthly", column="PICKUP_DATETIME", sort_ascending=False)
        query_daily = query_asset.add_batch_definition_daily(name="query_daily", column="PICKUP_DATETIME", sort_ascending=True)
        query_whole_table = query_asset.add_batch_definition_whole_table(name="whole_query")
        ```
        </td>
    </tr>
</table>