---
id: migration_guide
title: "GX V0 to V1 Migration Guide"
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

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

<Tabs 
   queryString="expectation_config"
   defaultValue="v0_expectation_suite_config"
   values={[
      {value: 'v0_expectation_suite_config', label: 'V0 Expectation Suite Configuration'},
      {value: 'v1_expectation_suite_config', label: 'V1 Expectation Suite Configuration'}
   ]}
>
    <TabItem value="v0_expectation_suite_config" label="V0 Expectation Suite Configuration">
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
    </TabItem>
    <TabItem value="v1_expectation_suite_config" label="V1 Expectation Suite Configuration">
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
    </TabItem>
</Tabs>

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

#### Expectation Suite API Calls

The suites above were created with the following API calls. This example demonstrates how to create an equivalent suite to your V0 suite in V1.

<Tabs 
   queryString="expectation_suite_api"
   defaultValue="v0_expectation_suite_api"
   values={[
      {value: 'v0_expectation_suite_api', label: 'V0 Expectation Suite API'},
      {value: 'v1_expectation_suite_api', label: 'V1 Expectation Suite API'}
   ]}
>
    <TabItem value="v0_expectation_suite_api" label="V0 Expectation Suite API">
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
    </TabItem>
    <TabItem value="v1_expectation_suite_api" label="V1 Expectation Suite API">
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
    </TabItem>
</Tabs>

### Data Sources and Data Assets
Data Source configurations are stored in the YAML file `gx/great_expectations.yml`, in the top-level block whose key is `fluent_datasources.` 

We’ll walk through examples of different Data Source configurations in V0 and V1 so you can see how to translate between the two.

#### Pandas API

##### Pandas Filesystem Data
Here is a side-by-side comparison of a Data Source called `pandas_fs_ds` with 4 assets called: `yearly_taxi_data`, `monthly_taxi_data`, `daily_taxi_data`, and `arbitrary_taxi_data`.

<Tabs 
   queryString="pandas_filesystem_config"
   defaultValue="v0_pandas_filesystem_config"
   values={[
      {value: 'v0_pandas_filesystem_config', label: 'V0 Pandas Filesystem Config'},
      {value: 'v1_pandas_filesystem_config', label: 'V1 Pandas Filesystem Config'}
   ]}
>
    <TabItem value="v0_pandas_filesystem_config" label="V0 Pandas Filesystem Config">
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
    </TabItem>
    <TabItem value="v1_pandas_filesystem_config" label="V1 Pandas Filesystem Config">
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
    </TabItem>
</Tabs>

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

<Tabs 
   queryString="pandas_filesystem_creation"
   defaultValue="v0_pandas_filesystem_creation"
   values={[
      {value: 'v0_pandas_filesystem_creation', label: 'V0 Pandas Filesystem Creation via API'},
      {value: 'v1_pandas_filesystem_creation', label: 'V1 Pandas Filesystem Creation via API'}
   ]}
>
    <TabItem value="v0_pandas_filesystem_creation" label="V0 Pandas Filesystem Creation via API">
    ```python
    # Pandas Filesystem Data Source
    datasource = context.sources.add_pandas_filesystem(name="pd_fs_ds", base_directory="data")

    # Data Assets
    yearly = datasource.add_csv_asset(name="yearly_taxi_data", batching_regex=r"sampled_yellow_tripdata_(?P<year>\d{4})\.csv")
    monthly = datasource.add_csv_asset(name="monthly_taxi_data", batching_regex=r"sampled_yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})\.csv")
    daily = datasource.add_csv_asset(name="daily_taxi_data", batching_regex=r"sampled_yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\.csv")
    arbitrary = datasource.add_csv_asset(name="arbitrary_taxi_data", batching_regex=r"sampled_yellow_tripdata_(?P<code>\w+)\.csv")
    ```
    </TabItem>
    <TabItem value="v1_pandas_filesystem_creation" label="V1 Pandas Filesystem Creation via API">
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
    </TabItem>
</Tabs>

##### Pandas Dataframe
<Tabs 
   queryString="pandas_dataframe_config"
   defaultValue="v0_pandas_dataframe_config"
   values={[
      {value: 'v0_pandas_dataframe_config', label: 'V0 Pandas Dataframe Config'},
      {value: 'v1_pandas_dataframe_config', label: 'V1 Pandas Dataframe Config'}
   ]}
>
    <TabItem value="v0_pandas_dataframe_config" label="V0 Pandas Dataframe Config">
    ```yaml
    fluent_datasources:
    pd_df_ds:
        type: pandas
        assets:
        taxi_dataframe_asset:
            type: dataframe
            batch_metadata: {}
    ```
    </TabItem>
    <TabItem value="v1_pandas_dataframe_config" label="V1 Pandas Dataframe Config">
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
    </TabItem>
</Tabs>

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
<Tabs 
   queryString="pandas_dataframe_creation"
   defaultValue="v0_pandas_dataframe_creation"
   values={[
      {value: 'v0_pandas_dataframe_creation', label: 'V0 Pandas Dataframe Creation via API'},
      {value: 'v1_pandas_dataframe_creation', label: 'V1 Pandas Dataframe Creation via API'}
   ]}
>
    <TabItem value="v0_pandas_dataframe_creation" label="V0 Pandas Dataframe Creation via API">
    ```python
    dataframe_ds = context.sources.add_pandas(name="pd_df_ds")
    dataframe_asset = dataframe_ds.add_dataframe_asset(name="taxi_dataframe_asset")
    ```
    </TabItem>
    <TabItem value="v1_pandas_dataframe_creation" label="V1 Pandas Dataframe Creation via API">
    ```python
    dataframe_ds = context.data_sources.add_pandas(name="pd_df_ds")
    dataframe_asset = dataframe_ds.add_dataframe_asset(name="taxi_dataframe_asset")
    dataframe_bd = dataframe_asset.add_batch_definition_whole_dataframe(name="taxi_dataframe_batch_def")
    ```
    </TabItem>
</Tabs>

#### Snowflake API
Here is a side-by-side comparision of a both a `V0` Snowflake table and query Data Asset to their equivalents in `V1`. We walk through all the currently supported V1 Batch Definitions: yearly, monthly, daily, and whole table.

<Tabs 
   queryString="snowflake_config"
   defaultValue="v0_snowflake_config"
   values={[
      {value: 'v0_snowflake_config', label: 'V0 Snowflake Config'},
      {value: 'v1_snowflake_config', label: 'V1 Snowflake Config'}
   ]}
>
    <TabItem value="v0_snowflake_config" label="V0 Snowflake Config">
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
    </TabItem>
    <TabItem value="v1_snowflake_config" label="V1 Snowflake Config">
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
    </TabItem>
</Tabs>

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
<Tabs 
   queryString="snowflake_creation"
   defaultValue="v0_snowflake_creation"
   values={[
      {value: 'v0_snowflake_creation', label: 'V0 Snowflake Creation via API'},
      {value: 'v1_snowflake_creation', label: 'V1 Snowflake Creation via API'}
   ]}
>
    <TabItem value="v0_snowflake_creation" label="V0 Snowflake Creation via API">
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
    </TabItem>
    <TabItem value="v1_snowflake_creation" label="V1 Snowflake Creation via API">
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
    </TabItem>
</Tabs>

#### Postgres API
The postgres Data Source/Asset migration from `V0` to `V1` is almost identical to the Snowflake one in terms of fields. All the fields are identical and how to migrate them from `V0` to `V1` is identical so please refer to the Snowflake section for a description. The differences in values are:
- The **type** field value is `postgres` instead of `snowflake` 
- We are NOT requiring schemas in V1 for postgres table assets.

Here is an example great_expectations.yml  fluent_datasources block and creation of this datasource and asset via the API.

The provided connection string is a sample dataset GX maintains.

Here is an example `great_expectations.yml` `fluent_datasources` block and creation of this datasource and asset via the API.

The provided connection string is a sample dataset GX maintains.

<Tabs 
   queryString="postgres_config"
   defaultValue="v0_postgres_config"
   values={[
      {value: 'v0_postgres_config', label: 'V0 Postgres Config'},
      {value: 'v1_postgres_config', label: 'V1 Postgres Config'}
   ]}
>
    <TabItem value="v0_postgres_config" label="V0 Postgres Config">
    ```yaml
    fluent_datasources:
        postgres_ds:
            type: postgres
            assets:
            yearly_taxi_data:
                type: table
                order_by:
                - key: year
                    reverse: false
                batch_metadata: {}
                splitter:
                column_name: pickup
                method_name: split_on_year
                table_name: nyc_taxi_data
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
                column_name: pickup
                method_name: split_on_year_and_month
                table_name: nyc_taxi_data
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
                column_name: pickup
                method_name: split_on_year_and_month_and_day
                table_name: nyc_taxi_data
                schema_name:
            all_taxi_data:
                type: table
                order_by: []
                batch_metadata: {}
                table_name: nyc_taxi_data
                schema_name:
            query_yearly:
                type: query
                order_by:
                - key: year
                    reverse: false
                batch_metadata: {}
                splitter:
                column_name: pickup
                method_name: split_on_year
                query: select * from nyc_taxi_data
            query_monthly:
                type: query
                order_by:
                - key: year
                    reverse: true
                - key: month
                    reverse: true
                batch_metadata: {}
                splitter:
                column_name: pickup
                method_name: split_on_year_and_month
                query: select * from nyc_taxi_data
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
                column_name: pickup
                method_name: split_on_year_and_month_and_day
                query: select * from nyc_taxi_data
            whole_query:
                type: query
                order_by: []
                batch_metadata: {}
                query: select * from nyc_taxi_data
            connection_string: postgresql+psycopg2://try_gx:try_gx@postgres.workshops.greatexpectations.io/gx_example_db
    ```
    </TabItem>
    <TabItem value="v1_postgres_config" label="V1 Postgres Config">
    ```yaml
    fluent_datasources:
        postgres_ds:
            type: postgres
            id: cc4984f4-dbad-4488-8b0a-47ec47fc294c
            assets:
            taxi_data:
                type: table
                id: cb140e3c-d33f-4920-9bfc-2a23de990283
                batch_metadata: {}
                batch_definitions:
                table_yearly:
                    id: 23e9d1c7-d22e-44f3-b1fa-eb0db1df4ce8
                    partitioner:
                    column_name: pickup
                    sort_ascending: true
                    method_name: partition_on_year
                table_monthly:
                    id: be939a11-a257-4f9a-83c8-8efd1b25d9c9
                    partitioner:
                    column_name: pickup
                    sort_ascending: false
                    method_name: partition_on_year_and_month
                table_daily:
                    id: 80fb4af2-2ab2-4a09-a05d-849835677c45
                    partitioner:
                    column_name: pickup
                    sort_ascending: true
                    method_name: partition_on_year_and_month_and_day
                whole_table:
                    id: 09674cda-573c-400b-9a64-10dcdaecb60b
                    partitioner:
                table_name: nyc_taxi_data
                schema_name:
            query_data:
                type: query
                id: 9ad6b38b-2337-4f51-bae2-31afb212c5f2
                batch_metadata: {}
                batch_definitions:
                query_yearly:
                    id: 56455714-0622-46b0-857f-60d964e1d004
                    partitioner:
                    column_name: pickup
                    sort_ascending: true
                    method_name: partition_on_year
                query_monthly:
                    id: e96513f1-12b8-419d-a1b9-4aacedfd396d
                    partitioner:
                    column_name: pickup
                    sort_ascending: false
                    method_name: partition_on_year_and_month
                query_daily:
                    id: 996a2813-6eff-4c8a-88c6-5ca9ab60e275
                    partitioner:
                    column_name: pickup
                    sort_ascending: true
                    method_name: partition_on_year_and_month_and_day
                whole_query:
                    id: f947cbc4-3d3b-4f92-bee0-4186fdac2b61
                    partitioner:
                query: SELECT * FROM nyc_taxi_data
            connection_string: postgresql+psycopg2://try_gx:try_gx@postgres.workshops.greatexpectations.io/gx_example_db
    ```
    </TabItem>
</Tabs>

##### Postgresql Creation via API
<Tabs 
   queryString="postgres_creation"
   defaultValue="v0_postgres_creation"
   values={[
      {value: 'v0_postgres_creation', label: 'V0 Postgres Creation via API'},
      {value: 'v1_postgres_creation', label: 'V1 Postgres Creation via API'}
   ]}
>
    <TabItem value="v0_postgres_creation" label="V0 Postgres Creation via API">
    ```python
    # Creating a datasource
    connection_string = "postgresql+psycopg2://try_gx:try_gx@postgres.workshops.greatexpectations.io/gx_example_db"
    ds = context.sources.add_postgres(name="postgres_ds", connection_string=connection_string)

    # Creating table assets
    yearly_asset = ds.add_table_asset(name="yearly_taxi_data", table_name="nyc_taxi_data", order_by=["+year"])
    yearly_asset.add_splitter_year(column_name="pickup")
    monthly_asset = ds.add_table_asset(name="monthly_taxi_data", table_name="nyc_taxi_data", order_by=["-year", "-month"])
    monthly_asset.add_splitter_year_and_month(column_name="pickup")
    daily_asset = ds.add_table_asset(name="daily_taxi_data", table_name="nyc_taxi_data", order_by=["+year", "+month", "+day"])
    daily_asset.add_splitter_year_and_month_and_day(column_name="pickup")
    whole_table_asset = ds.add_table_asset(name="all_taxi_data", table_name="nyc_taxi_data")

    # Creating query Assets
    yearly_query_asset = ds.add_query_asset(name="query_yearly", query="select * from nyc_taxi_data", order_by=["+year"])
    yearly_query_asset.add_splitter_year(column_name="pickup")
    monthly_query_asset = ds.add_query_asset(name="query_monthly", query="select * from nyc_taxi_data", order_by=["-year", "-month"])
    monthly_query_asset.add_splitter_year_and_month(column_name="pickup")
    daily_query_asset = ds.add_query_asset(name="query_daily", query="select * from nyc_taxi_data", order_by=["+year", "+month", "+day"])
    daily_query_asset.add_splitter_year_and_month_and_day(column_name="pickup")
    query_whole_table_asset = ds.add_query_asset(name="whole_query", query="select * from nyc_taxi_data")
    ```
    </TabItem>
    <TabItem value="v1_postgres_creation" label="V1 Postgres Creation via API">
    ```python 
    # Creating a datasource
    connection_string = "postgresql+psycopg2://try_gx:try_gx@postgres.workshops.greatexpectations.io/gx_example_db"
    ds = context.data_sources.add_postgres(name="postgres_ds", connection_string=connection_string)

    # Creating a table asset and batch definitions
    table_asset = ds.add_table_asset(name="taxi_data", table_name="nyc_taxi_data")
    table_yearly = table_asset.add_batch_definition_yearly(name="table_yearly", column="pickup", sort_ascending=True)
    table_monthly = table_asset.add_batch_definition_monthly(name="table_monthly", column="pickup", sort_ascending=False)
    table_daily = table_asset.add_batch_definition_daily(name="table_daily", column="pickup", sort_ascending=True)
    whole_table = table_asset.add_batch_definition_whole_table(name="whole_table")

    # Creating a query asset and batch definitions
    query_asset = ds.add_query_asset(name="query_data", query="SELECT * FROM nyc_taxi_data")
    query_yearly = query_asset.add_batch_definition_yearly(name="query_yearly", column="pickup", sort_ascending=True)
    query_monthly = query_asset.add_batch_definition_monthly(name="query_monthly", column="pickup", sort_ascending=False)
    query_daily = query_asset.add_batch_definition_daily(name="query_daily", column="pickup", sort_ascending=True)
    query_whole_table = query_asset.add_batch_definition_whole_table(name="whole_query")
    ```
    </TabItem>
</Tabs>

#### Spark API
##### Spark Filesystem
This is almost identical to the pandas filesystem and we only present a daily and a yearly asset conversion here.

<Tabs 
   queryString="spark_filesystem_config"
   defaultValue="v0_spark_filesystem_config"
   values={[
      {value: 'v0_spark_filesystem_config', label: 'V0 Spark Filesystem Config'},
      {value: 'v1_spark_filesystem_config', label: 'V1 Spark Filesystem Config'}
   ]}
>
    <TabItem value="v0_spark_filesystem_config" label="V0 Spark Filesystem Config">
    ```yaml
    fluent_datasources:
        spark_fs:
            type: spark_filesystem
            assets:
            yearly_taxi_data:
                type: csv
                batching_regex: sampled_yellow_tripdata_(?P<year>\d{4})\.csv
            daily_taxi_data:
                type: csv
                batching_regex: sampled_yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\.csv
            spark_config:
            spark.executor.memory: 4g
            persist: true
            base_directory: data
    ```
    </TabItem>
    <TabItem value="v1_spark_filesystem_config" label="V1 Spark Filesystem Config">
    ```yaml
    fluent_datasources:
        spark_fs:
            type: spark_filesystem
            id: 62a7c671-8f2a-468c-be53-a82576d7b436
            assets:
            taxi_data:
                type: csv
                id: 78d5ccc2-1697-490f-886a-c9672d5548c6
                batch_definitions:
                yearly_batches:
                    id: 4a0ff04f-a9fe-4c36-b680-0b1c61f4e0c2
                    partitioner:
                    regex: sampled_yellow_tripdata_(?P<year>\d{4})\.csv
                    sort_ascending: true
                daily_batches:
                    id: b2e056fe-6f1d-4fdc-ab69-75d3a19f1a44
                    partitioner:
                    regex: sampled_yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\.csv
                    sort_ascending: true
                    param_names:
                        - year
                        - month
                        - day
            spark_config:
            spark.executor.memory: 4g
            persist: true
            base_directory: data
    ```
    </TabItem>
</Tabs>

##### Spark Filesystem API
<Tabs 
   queryString="spark_filesystem_api"
   defaultValue="v0_spark_filesystem_api"
   values={[
      {value: 'v0_spark_filesystem_api', label: 'V0 Spark Filesystem API'},
      {value: 'v1_spark_filesystem_api', label: 'V1 Spark Filesystem API'}
   ]}
>
    <TabItem value="v0_spark_filesystem_api" label="V0 Spark Filesystem API">
    ```python
    import great_expectations as gx
    context = gx.get_context(mode="file")

    datasource = context.sources.add_spark_filesystem(name="spark_fs", base_directory="data", spark_config={"spark.executor.memory": "4g"}, persist=True)
    yearly = datasource.add_csv_asset(name="yearly_taxi_data", batching_regex=r"sampled_yellow_tripdata_(?P<year>\d{4})\.csv")
    daily = datasource.add_csv_asset(name="daily_taxi_data", batching_regex=r"sampled_yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\.csv")
    ```
    </TabItem>
    <TabItem value="v1_spark_filesystem_api" label="V1 Spark Filesystem API">
    ```python
    import great_expectations as gx
    context = gx.get_context(mode="file")

    data_source = context.data_sources.add_spark_filesystem(name="spark_fs", base_directory="data", spark_config={"spark.executor.memory": "4g"}, persist=True)
    asset = data_source.add_csv_asset(name="taxi_data")
    yearly = asset.add_batch_definition_yearly(name="yearly_batches", regex=r"sampled_yellow_tripdata_(?P<year>\d{4})\.csv")
    daily = asset.add_batch_definition_daily(name="daily_batches", regex=r"sampled_yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\.csv")
    ```
    </TabItem>
</Tabs>

##### Spark Dataframe
Here is a side-by-side comparison of the Spark dataframe data source configuration. 

<Tabs 
   queryString="spark_dataframe_config"
   defaultValue="v0_spark_dataframe_config"
   values={[
      {value: 'v0_spark_dataframe_config', label: 'V0 Spark Dataframe Config'},
      {value: 'v1_spark_dataframe_config', label: 'V1 Spark Dataframe Config'}
   ]}
>
    <TabItem value="v0_spark_dataframe_config" label="V0 Spark Dataframe Config">
    ```yaml
    fluent_datasources:
        spark_ds:
            type: spark
            assets:
            taxi_dataframe_asset:
                type: dataframe
                batch_metadata: {}
            spark_config:
            spark.executor.memory: 4g
            force_reuse_spark_context: true
            persist: true
    ```
    </TabItem>
    <TabItem value="v1_spark_dataframe_config" label="V1 Spark Dataframe Config">
    ```yaml
    fluent_datasources:
        spark_ds:
            type: spark
            id: 134de28d-bfdc-4980-aa2e-4f59788afef3
            assets:
            taxi_dataframe_asset:
                type: dataframe
                id: 4110d2ff-5711-47df-a4be-eaefc2a638b4
                batch_metadata: {}
                batch_definitions:
                taxi_dataframe_batch_def:
                    id: 76738b8b-28ab-4857-aa98-f0ff80c8f137
                    partitioner:
            spark_config:
            spark.executor.memory: 4g
            force_reuse_spark_context: true
            persist: true
    ```
    </TabItem>
</Tabs>

##### Spark dataframe API
<Tabs 
   queryString="spark_dataframe_api"
   defaultValue="v0_spark_dataframe_api"
   values={[
      {value: 'v0_spark_dataframe_api', label: 'V0 Spark Dataframe API'},
      {value: 'v1_spark_dataframe_api', label: 'V1 Spark Dataframe API'}
   ]}
>
    <TabItem value="v0_spark_dataframe_api" label="V0 Spark Dataframe API">
    ```python
    import great_expectations as gx
    context = gx.get_context(mode="file")

    dataframe_ds = context.sources.add_spark(name="spark_ds", spark_config={"spark.executor.memory": "4g"}, force_reuse_spark_context=True, persist=True)
    dataframe_asset = dataframe_ds.add_dataframe_asset(name="taxi_dataframe_asset")
    ```
    </TabItem>
    <TabItem value="v1_spark_dataframe_api" label="V1 Spark Dataframe API">
    ```python
    import great_expectations as gx
    context = gx.get_context(mode="file")

    dataframe_ds = context.data_sources.add_spark(name="spark_ds", spark_config={"spark.executor.memory": "4g"}, force_reuse_spark_context=True, persist=True)
    dataframe_asset = dataframe_ds.add_dataframe_asset(name="taxi_dataframe_asset")
    dataframe_bd = dataframe_asset.add_batch_definition_whole_dataframe(name=ƒ"taxi_dataframe_batch_def")
    ```
    </TabItem>
</Tabs>

#### Spark Directory Asset
Spark directory assets are different than our other dataframe Data Assets. These assets pull all the files from a directory into a single dataframe. Then, like for SQL Data Sources, one specifies a column when adding the Batch Definition. This column will be used to split the dataframe into batches.

For this example all the data files live in directory `data/data2/` relative to our project directory.

In V0, we split the data based on an exact string. In V1, our batch definitions are all based on datetime (eg batches are by day, month, or year). 

<Tabs 
   queryString="spark_directory_asset_config"
   defaultValue="v0_spark_directory_asset_config"
   values={[
      {value: 'v0_spark_directory_asset_config', label: 'V0 Spark Directory Asset Config'},
      {value: 'v1_spark_directory_asset_config', label: 'V1 Spark Directory Asset Config'}
   ]}
>
    <TabItem value="v0_spark_directory_asset_config" label="V0 Spark Directory Asset Config">
    ```yaml
    fluent_datasources:
        spark:
            type: spark_filesystem
            assets:
            spark_asset:
                type: directory_csv
                header: true
                data_directory: data2
            spark_config:
            spark.executor.memory: 4g
            persist: true
            base_directory: data
    ```
    </TabItem>
    <TabItem value="v1_spark_directory_asset_config" label="V1 Spark Directory Asset Config">
    ```yaml
    fluent_datasources:
        spark:
            type: spark_filesystem
            id: a35e995d-dd60-45e4-90f0-061d2bda6544
            assets:
            spark_asset:
                type: directory_csv
                id: 9454840d-f064-4129-b8ff-38cfbb71af99
                batch_definitions:
                monthly:
                    id: 853d02de-54b1-45a7-a4e2-b9f8a8ca0a33
                    partitioner:
                    column_name: tpep_pickup_datetime
                    method_name: partition_on_year_and_month
                header: true
                data_directory: data2
            spark_config:
            spark.executor.memory: 4g
            persist: true
            base_directory: data
    ```
    </TabItem>
</Tabs>

The configuration for `0.X` because we only allow splitting the data into batches by exact string match and we require uses to fully specify the batch request options in GX `0.X` (batch parameters in GX `1.0`). I am not showing all the spark specific configuration options. They are both supported in the same way in GX `0.X` and GX `1.0`.

##### Spark directory asset API
<Tabs 
   queryString="spark_directory_asset_api"
   defaultValue="v0_spark_directory_asset_api"
   values={[
      {value: 'v0_spark_directory_asset_api', label: 'V0 Spark Directory Asset API'},
      {value: 'v1_spark_directory_asset_api', label: 'V1 Spark Directory Asset API'}
   ]}
>
    <TabItem value="v0_spark_directory_asset_api" label="V0 Spark Directory Asset API">
    ```python
    import great_expectations as gx

    context = gx.get_context(mode="file")

    ds = context.sources.add_spark_filesystem(name="spark", base_directory="data", spark_config={"spark.executor.memory": "4g"}, persist=True)

    asset = ds.add_directory_csv_asset(name="spark_asset", data_directory="data2", header=True)
    # This must really be a year-month date column instead of a datetime column for splitting by month in GX 0.X
    asset.add_splitter_column_value(column_name="tpep_pickup_datetime")
    # There is no sorting added because in GX 0.X, one has to specify all parameters so sorting is a no-op

    ```
    </TabItem>
    <TabItem value="v1_spark_directory_asset_api" label="V1 Spark Directory Asset API">
    ```python
    import great_expectations as gx

    context = gx.get_context(mode="file")
    ds = context.data_sources.add_spark_filesystem(name="spark", base_directory="data", spark_config={"spark.executor.memory": "4g"}, persist=True)
    asset = ds.add_directory_csv_asset(name="spark_asset", data_directory="data2", header=True)
    bd = asset.add_batch_definition_monthly(name="monthly", column="tpep_pickup_datetime")

    b = bd.get_batch()
    b.head(fetch_all=True)

    ```
    </TabItem>
</Tabs>

### Checkpoints
In V0, there were multiple equivalent ways to configure the exact same `Checkpoint`. This is because a `Checkpoint` object contained a `validations` parameter which was a list of the validations the `Checkpoint` would run. Each item in this list took all the arguments necessary for a validation such as the Expectation Suite, the Batch Request, the actions, etc. However, all these same arguments are also present on the `Checkpoint` initializer. Usually, if an argument was present in the validation, that would be used, but if any argument was not present in a validation, GX would fall back to the argument defined on the `Checkpoint` itself. We’d call these default values the “top-level values”. In addition, if the `validations` argument was an empty list or `None`, GX would infer the `Checkpoint` had 1 validation and create one using only “top-level values”. In this case, we’d call this validation a “top-level validation”. This fallback led to some confusing behavior, especially since it wasn’t consistently implemented. 

In V1, we have removed all top-level arguments so every validation must be fully specified in the `validation_definitions` argument which is the analog to the old `validations` argument. We’ve also promoted the Validation Definition to its own domain object since it encapsulates the unit of validation. Checkpoints are groupings of Validation Definitions packaged with actions that may be taken after a validation is run. With this in mind, the V0 checkpoint configuration has been broken into 2 files, a Validation Definition configuration file and a checkpoint configuration file.

We walk through 4 cases of V0 configuration files:

Case 1: An empty validations argument so only a top-level validation exists.

Case 2: No top-level validations so all values come from the validations argument.

Case 3: A validation with values specified both in the validation and on the top level.

Case 4: A validation with values specified on the top level that is overridden in the validation.

We hope that this gives enough breadth over the possible ways to convert a Checkpoint that a migrator will have a helpful example. If there are missing cases that you’d like to see appear, please reach out.

#### Case 1: Empty Validations Argument
The V0 configuration lives in `gx/checkpoints/<CHECKPOINT_NAME>.yml`. In V1, the configuration is JSON and lives in 2 files: `gx/checkpoints/<CHECKPOINT_NAME>` and `gx/validation_definitions/<VALIDATION_DEFINITION_NAME>`.

<Tabs 
   queryString="checkpoints_yaml"
   defaultValue="v0_checkpoints_yaml"
   values={[
      {value: 'v0_checkpoints_yaml', label: 'V0: gx/checkpoints/my_checkpoint.yml'},
      {value: 'v1_checkpoints_yaml', label: 'V1: gx/checkpoints/my_checkpoint and gx/validation_definitions/my_validation_definition'}
   ]}
>
    <TabItem value="v0_checkpoints_yaml" label="V0: gx/checkpoints/my_checkpoint.yml">
    ```yaml
    name: my_checkpoint
    config_version: 1.0
    template_name:
    module_name: great_expectations.checkpoint
    class_name: Checkpoint
    run_name_template:
    expectation_suite_name: my_suite
    batch_request:
    datasource_name: pd_fs_ds
    data_asset_name: monthly_taxi_data
    action_list:
    - name: store_validation_result
        action:
        class_name: StoreValidationResultAction
    - name: store_evaluation_params
        action:
        class_name: StoreEvaluationParametersAction
    - name: update_data_docs
        action:
        class_name: UpdateDataDocsAction
    - name: my_email_action
        action:
        class_name: EmailAction
        notify_on: all
        use_tls: true
        use_ssl: false
        renderer:
            module_name: great_expectations.render.renderer.email_renderer
            class_name: EmailRenderer
        smtp_address: smtp.myserver.com
        smtp_port: 587
        sender_login: sender@myserver.com
        sender_password: XXXXXXXXXX
        sender_alias: alias@myserver.com
        receiver_emails: receiver@myserver.com
    evaluation_parameters: {}
    runtime_configuration: {}
    validations: []
    profilers: []
    ge_cloud_id:
    expectation_suite_ge_cloud_id:
    ```
    </TabItem>
    <TabItem value="v1_checkpoints_yaml" label="V1: gx/checkpoints/my_checkpoint and gx/validation_definitions/my_validation_definition">
    **gx/validation_definitions/my_validation_definition**
    ```json
    {
        "data": {
            "asset": {
            "id": "ae696e27-fb6a-45fb-a2a0-bf1b8627c07e",
            "name": "taxi_data"
            },
            "batch_definition": {
            "id": "9b396884-ef73-47f5-b8f7-c2fc1306589b",
            "name": "monthly_batches"
            },
            "datasource": {
            "id": "934fd0e2-4c34-4e88-be1a-6b56ed69d614",
            "name": "pd_fs_ds"
            }
        },
        "id": "cbd6552b-12d4-4b9f-92d5-1223eb6730d8",
        "name": "my_validation_definition",
        "suite": {
            "id": "a71b700d-867a-46be-b5f2-6b9402dcc925",
            "name": "my_suite"
        }
    }
    ```

    **gx/checkpoints/my_checkpoint**
    ```json
    {
        "actions": [
            {
            "name": "update_data_docs",
            "site_names": [],
            "type": "update_data_docs"
            },
            {
            "name": "my_email_action",
            "notify_on": "all",
            "notify_with": null,
            "receiver_emails": "receiver@myserver.com",
            "renderer": {
                "class_name": "EmailRenderer",
                "module_name": "great_expectations.render.renderer.email_renderer"
            },
            "sender_alias": "alias@myserver.com",
            "sender_login": "sender@myserver.com",
            "sender_password": "XXXXXXXXXX",
            "smtp_address": "smtp.myserver.com",
            "smtp_port": "587",
            "type": "email",
            "use_ssl": false,
            "use_tls": true
            }    
        ],
        "id": "ff7a0cd3-6b64-463a-baa0-4b5b4d7512b5",
        "name": "my_checkpoint",
        "result_format": "SUMMARY",
        "validation_definitions": [
            {
            "id": "cbd6552b-12d4-4b9f-92d5-1223eb6730d8",
            "name": "my_validation_definition"
            }
        ]
        } 
    ```
    </TabItem>
</Tabs>

We provide a mapping from the V0 fields to the V1 fields along with any new V1 fields.

**name**: This gets mapped to the name field in the V1 Checkpoint configuration file.

**config_version**: This is no longer a parameter.

**template_name**: This is no longer a supported feature. If you need to migrate this over, you should find the template values and set them explicitly in the new Checkpoint.

**module_name**: This is no longer necessary and is inferred so is no longer a supported parameter.

**class_name**: This is no longer necessary and is inferred so is no longer a supported parameter.

**run_name_template**: This is no longer a supported feature.

**expectation_suite_name**: This is now found in the validation definition configuration in **suite.name**.

**batch_request**: There is no longer a batch request concept in V1. The Data Source and Data Asset are now found in the Validation Definition configuration **data** field. The data field has 3 keys: **asset**, **batch_definition**, and **datasource**. The value is a dictionary with the keys:

> **name**: The name of the asset/batch_definition/datasource found in great_expectations.yml.

> **id**: The id for the asset/batch_definition/datasource found in great_expectations.yml.

**action_list**: This is now mapped to the checkpoint configurations actions key which is a list of dictionaries where each dictionary configures one action. The name for an action in a V0 action list maps to the V1 action dictionary name key. A few things to note:
- V1 has no default actions.
- The `store_validation_result` is no longer an action since validation results are always stored and this is built into running a checkpoint (and running a validation definition directly).
- The `store_evaluation_params` action no longer exists since runtime parameters must now be passed in at runtime so we don’t store defaults anywhere.
- The `update_data_docs` action is no longer automatically added and must be explicitly added. Its configuration is a list of **site_names**. If you’ve configured these in V0, you can move them over directly and they have the same values. There is a new field called type, which all actions have, that is a unique literal string for a particular action. For this action type should be set to “update_data_docs”.

**evaluation_parameters**: This is no longer supported at the checkpoint level. In V0 one could also configure evaluation_parameters in the expectation suite parameters. One can still do that there (now called suite_parameters, see the Expectation Suites and Expectations section) and using that Expectation Suite will enable these parameters for checkpoints using that suite.

**runtime_configuration**: The runtime configuration supported by V1 is result format. There is now an explicit result_format key in the checkpoint configuration whose value is one of the following strings: SUMMARY, COMPLETE, BASIC, BOOLEAN_ONLY.

**validations**: This is now the checkpoint configuration field **validation_definitions** which is a list of dictionaries where each item in the list corresponds to a validation definition. There are 2 keys in the Validation Definition dictionary:

> **id**: This must match the top-level id field in the validation_definitions configuration file that corresponds to this validation definition.

> **name**: This must match the top-level name field in the validation_definitions configuration file that corresponds to this validation definition.

> There are now restrictions on which validations can be grouped together in a checkpoint. Each Validation Definition in a Checkpoint must take the same batch parameters at runtime. So if you grouped together multiple validations together in V0 whose batches are parameterized differently (e.g. one uses a “whole table” batch definition and another uses a “daily” batch definition) they will have to be split up into multiple checkpoints.

**profilers**: This feature has been removed in V1. Some form of profilers will be re-introduced in V1 at a later date.

**ge_cloud_id**: This should be empty for file-based configurations and has been removed in V1.

**expectation_suite_ge_cloud_id**: This should be empty for file based configurations and has been removed in V1.

##### Case 1: API calls
<Tabs 
   queryString="checkpoint_case_1"
   defaultValue="v0_checkpoint_api"
   values={[
      {value: 'v0_checkpoint_api', label: 'V0 Checkpoint API'},
      {value: 'v1_checkpoint_api', label: 'V1 Checkpoint API'}
   ]}
>
    <TabItem value="v0_checkpoint_api" label="V0 Checkpoint API">
    ```python
    import great_expectations as gx
    from great_expectations.checkpoint.actions import EmailAction

    context = gx.get_context(mode="file")


    datasource = context.sources.add_pandas_filesystem(name="pd_fs_ds", base_directory="data")
    monthly = datasource.add_csv_asset(name="monthly_taxi_data", batching_regex=r"sampled_yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})\.csv")

    suite = context.add_expectation_suite(
        expectation_suite_name="my_suite",
        data_asset_type="CSVAsset",
    )

    validator = context.get_validator(batch_request=monthly.build_batch_request(), expectation_suite_name="my_suite")
    validator.expect_column_values_to_be_between(column="passenger_count", min_value=0, max_value=10)
    validator.save_expectation_suite(discard_failed_expectations=False)


    batch_request = monthly.build_batch_request()  # options={"year": "2019", "month": "01"})

    email_action_config = {
        "name": "my_email_action",
        "action": {
            "class_name": "EmailAction",
            "notify_on": "all",
            "use_tls": True,
            "use_ssl": False,
            "renderer": {
                "module_name": "great_expectations.render.renderer.email_renderer",
                "class_name": "EmailRenderer"
            },
            "smtp_address": "smtp.myserver.com",
            "smtp_port": 587,
            "sender_login": "sender@myserver.com",
            "sender_password": "XXXXXXXXXX",
            "sender_alias": "alias@myserver.com",
            "receiver_emails": "receiver@myserver.com",
        }
    }

    action_list = [
        {'name': 'store_validation_result',
        'action': {'class_name': 'StoreValidationResultAction'}},
        {'name': 'store_evaluation_params',
        'action': {'class_name': 'StoreEvaluationParametersAction'}},
        {'name': 'update_data_docs',
        'action': {'class_name': 'UpdateDataDocsAction'}},
        email_action_config
    ]

    checkpoint_config = {
        "name": "my_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "module_name": "great_expectations.checkpoint",
        "expectation_suite_name": "my_suite",
        "batch_request": batch_request,
        "action_list": action_list,
    }

    checkpoint = context.add_checkpoint(**checkpoint_config)
    result = context.run_checkpoint("my_checkpoint")
    ```
    </TabItem>
    <TabItem value="v1_checkpoint_api" label="V1 Checkpoint API">
    ```python
    import great_expectations as gx
    import great_expectations.expectations as gxe

    context = gx.get_context(mode="file")
    data_source = context.data_sources.add_pandas_filesystem(name="pd_fs_ds", base_directory="./data")
    file_csv_asset = data_source.add_csv_asset(name="taxi_data")
    monthly = file_csv_asset.add_batch_definition_monthly(name="monthly_batches", regex=r"sampled_yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})\.csv")

    suite = context.suites.add(gx.ExpectationSuite(name="my_suite"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="passenger_count", min_value=0, max_value=10))

    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(data=monthly, suite=suite, name="my_validation_definition")
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="my_checkpoint",
            validation_definitions=[validation_definition],
            actions=[
                gx.checkpoint.UpdateDataDocsAction(name="update_data_docs"),
                gx.checkpoint.EmailAction(
                    name="my_email_action",
                    notify_on="all",
                    use_tls=True,
                    use_ssl=False,
                    smtp_address="smtp.myserver.com",
                    smtp_port=587,
                    sender_login="sender@myserver.com",
                    sender_password="XXXXXXXXXX",
                    sender_alias="alias@myserver.com",
                    receiver_emails="receiver@myserver.com",
                ),
            ],
        )
    )
    result = checkpoint.run()
    ```
    </TabItem>
</Tabs>

##### Case 2: No top-level arguments

We only show the V0 configuration and code samples here because the V1 configuration and code is identical to case 1.

One unique thing to notice is that while in the API code snippet below all actions are defined in the validation argument, you will see in the configuration file that the actions get split up and some end up being defined on the top level and some appear on the validation. All actions will get run when the checkpoint is run, which is inconsistent with the normal “overriding” behavior for values defined in the validation.

<Tabs 
   queryString="checkpoint_case_2"
   defaultValue="v0_checkpoint_case_2"
   values={[
      {value: 'v0_checkpoint_case_2', label: 'V0 Checkpoint Configuration'}
   ]}
>
    <TabItem value="v0_checkpoint_case_2" label="V0 Checkpoint Configuration">
    ```yaml
    name: my_checkpoint
    config_version: 1.0
    template_name:
    module_name: great_expectations.checkpoint
    class_name: Checkpoint
    run_name_template:
    expectation_suite_name:
    batch_request: {}
    action_list:
        - name: store_validation_result
            action:
            class_name: StoreValidationResultAction
        - name: store_evaluation_params
            action:
            class_name: StoreEvaluationParametersAction
        - name: update_data_docs
            action:
            class_name: UpdateDataDocsAction
    evaluation_parameters: {}
    runtime_configuration: {}
    validations:
    - action_list:
        - name: store_validation_result
            action:
            class_name: StoreValidationResultAction
        - name: store_evaluation_params
            action:
            class_name: StoreEvaluationParametersAction
        - name: update_data_docs
            action:
            class_name: UpdateDataDocsAction
        - name: my_email_action
            action:
            class_name: EmailAction
            notify_on: all
            use_tls: true
            use_ssl: false
            renderer:
                module_name: great_expectations.render.renderer.email_renderer
                class_name: EmailRenderer
            smtp_address: smtp.myserver.com
            smtp_port: 587
            sender_login: sender@myserver.com
            sender_password: XXXXXXXXXX
            sender_alias: alias@myserver.com
            receiver_emails: receiver@myserver.com
        batch_request:
        datasource_name: pd_fs_ds
        data_asset_name: monthly_taxi_data
        options: {}
        batch_slice:
        expectation_suite_name: my_suite
    profilers: []
    ge_cloud_id:
    expectation_suite_ge_cloud_id:
    ```
    </TabItem>
</Tabs>

##### Case 2: API calls

<Tabs 
   queryString="checkpoint_case_2_api"
   defaultValue="v0_checkpoint_case_2_api"
   values={[
      {value: 'v0_checkpoint_case_2_api', label: 'V0 Checkpoint API'}
   ]}
>
    <TabItem value="v0_checkpoint_case_2_api" label="V0 Checkpoint API">
    ```python
    import great_expectations as gx

    context = gx.get_context(mode="file")

    datasource = context.sources.add_pandas_filesystem(name="pd_fs_ds", base_directory="data")
    monthly = datasource.add_csv_asset(name="monthly_taxi_data", batching_regex=r"sampled_yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})\.csv")

    suite = context.add_expectation_suite(
        expectation_suite_name="my_suite",
        data_asset_type="CSVAsset",
    )
    validator = context.get_validator(batch_request=monthly.build_batch_request(), expectation_suite_name="my_suite")
    validator.expect_column_values_to_be_between(column="passenger_count", min_value=0, max_value=10)
    validator.save_expectation_suite(discard_failed_expectations=False)

    batch_request = monthly.build_batch_request()

    email_action_config = {
        "name": "my_email_action",
        "action": {
            "class_name": "EmailAction",
            "notify_on": "all",
            "use_tls": True,
            "use_ssl": False,
            "renderer": {
                "module_name": "great_expectations.render.renderer.email_renderer",
                "class_name": "EmailRenderer"
            },
            "smtp_address": "smtp.myserver.com",
            "smtp_port": 587,
            "sender_login": "sender@myserver.com",
            "sender_password": "XXXXXXXXXX",
            "sender_alias": "alias@myserver.com",
            "receiver_emails": "receiver@myserver.com",
        }
    }

    action_list = [
        {'name': 'store_validation_result',
        'action': {'class_name': 'StoreValidationResultAction'}},
        {'name': 'store_evaluation_params',
        'action': {'class_name': 'StoreEvaluationParametersAction'}},
        {'name': 'update_data_docs',
        'action': {'class_name': 'UpdateDataDocsAction'}},
        email_action_config
    ]

    checkpoint_config = {
        "name": "my_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "module_name": "great_expectations.checkpoint",
        "validations": [
            {
                "expectation_suite_name": "my_suite",
                "batch_request": batch_request,
                "action_list": action_list,
            }
        ],
    }

    checkpoint = context.add_checkpoint(**checkpoint_config)
    result_case_2 = context.run_checkpoint("my_checkpoint")
    ```
    </TabItem>
</Tabs>

##### Case 3: Combined top level and validation configuration
We only show the V0 configuration and code samples here because the V1 configuration and code is identical to case 1.

<Tabs 
   queryString="checkpoint_case_3"
   defaultValue="v0_checkpoint_case_3"
   values={[
      {value: 'v0_checkpoint_case_3', label: 'V0 Checkpoint Configuration'}
   ]}
>
    <TabItem value="v0_checkpoint_case_3" label="V0 Checkpoint Configuration">
    ```yaml
    name: top_level_and_validation_checkpoint
    config_version: 1.0
    template_name:
    module_name: great_expectations.checkpoint
    class_name: Checkpoint
    run_name_template:
    expectation_suite_name: my_suite
    batch_request: {}
    action_list:
    - name: store_validation_result
        action:
        class_name: StoreValidationResultAction
    - name: store_evaluation_params
        action:
        class_name: StoreEvaluationParametersAction
    - name: update_data_docs
        action:
        class_name: UpdateDataDocsAction
    - name: my_email_action
        action:
        class_name: EmailAction
        notify_on: all
        use_tls: true
        use_ssl: false
        renderer:
            module_name: great_expectations.render.renderer.email_renderer
            class_name: EmailRenderer
        smtp_address: smtp.myserver.com
        smtp_port: 587
        sender_login: sender@myserver.com
        sender_password: XXXXXXXXXX
        sender_alias: alias@myserver.com
        receiver_emails: receiver@myserver.com
    evaluation_parameters: {}
    runtime_configuration: {}
    validations:
    - batch_request:
        datasource_name: pd_fs_ds
        data_asset_name: monthly_taxi_data
        options: {}
        batch_slice:
    profilers: []
    ge_cloud_id:
    expectation_suite_ge_cloud_id:
    ```
    </TabItem>
</Tabs>

##### Case 3: API calls

<Tabs 
   queryString="checkpoint_case_3_api"
   defaultValue="v0_checkpoint_case_3_api"
   values={[
      {value: 'v0_checkpoint_case_3_api', label: 'V0 Checkpoint API'}
   ]}
>
    <TabItem value="v0_checkpoint_case_3_api" label="V0 Checkpoint API">
    ```python
    import great_expectations as gx

    context = gx.get_context(mode="file")

    datasource = context.sources.add_pandas_filesystem(name="pd_fs_ds", base_directory="data")
    monthly = datasource.add_csv_asset(name="monthly_taxi_data", batching_regex=r"sampled_yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})\.csv")

    suite = context.add_expectation_suite(
        expectation_suite_name="my_suite",
        data_asset_type="CSVAsset",
    )
    validator = context.get_validator(batch_request=monthly.build_batch_request(), expectation_suite_name="my_suite")
    validator.expect_column_values_to_be_between(column="passenger_count", min_value=0, max_value=10)
    validator.save_expectation_suite(discard_failed_expectations=False)

    batch_request = monthly.build_batch_request()

    email_action_config = {
        "name": "my_email_action",
        "action": {
            "class_name": "EmailAction",
            "notify_on": "all",
            "use_tls": True,
            "use_ssl": False,
            "renderer": {
                "module_name": "great_expectations.render.renderer.email_renderer",
                "class_name": "EmailRenderer"
            },
            "smtp_address": "smtp.myserver.com",
            "smtp_port": 587,
            "sender_login": "sender@myserver.com",
            "sender_password": "XXXXXXXXXX",
            "sender_alias": "alias@myserver.com",
            "receiver_emails": "receiver@myserver.com",
        }
    }

    action_list = [
        {'name': 'store_validation_result',
        'action': {'class_name': 'StoreValidationResultAction'}},
        {'name': 'store_evaluation_params',
        'action': {'class_name': 'StoreEvaluationParametersAction'}},
        {'name': 'update_data_docs',
        'action': {'class_name': 'UpdateDataDocsAction'}},
        email_action_config
    ]

    checkpoint_config = {
        "name": "top_level_and_validation_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "module_name": "great_expectations.checkpoint",
        "expectation_suite_name": "my_suite",
        "action_list": action_list,
        "validations": [
            {
                "batch_request": batch_request,
            }
        ],
    }

    context.add_checkpoint(**checkpoint_config)
    result = context.run_checkpoint("top_level_and_validation_checkpoint")
    ```
    </TabItem>
</Tabs>

##### Case 4: Combined top level with validation configuration override
We only show the V0 configuration and code samples here because the V1 configuration and code is identical to case 1.

<Tabs 
   queryString="checkpoint_case_4"
   defaultValue="v0_checkpoint_case_4"
   values={[
      {value: 'v0_checkpoint_case_4', label: 'V0 Checkpoint Configuration'}
   ]}
>
    <TabItem value="v0_checkpoint_case_4" label="V0 Checkpoint Configuration">
    ```yaml
    name: top_level_and_validation_override_checkpoint
    config_version: 1.0
    template_name:
    module_name: great_expectations.checkpoint
    class_name: Checkpoint
    run_name_template:
    expectation_suite_name: my_suite
    batch_request: {}
    action_list:
    - name: store_validation_result
        action:
        class_name: StoreValidationResultAction
    - name: store_evaluation_params
        action:
        class_name: StoreEvaluationParametersAction
    - name: update_data_docs
        action:
        class_name: UpdateDataDocsAction
    - name: my_email_action
        action:
        class_name: EmailAction
        notify_on: all
        use_tls: true
        use_ssl: false
        renderer:
            module_name: great_expectations.render.renderer.email_renderer
            class_name: EmailRenderer
        smtp_address: smtp.myserver.com
        smtp_port: 587
        sender_login: sender@myserver.com
        sender_password: XXXXXXXXXX
        sender_alias: alias@myserver.com
        receiver_emails: receiver@myserver.com
    evaluation_parameters: {}
    runtime_configuration: {}
    validations:
    - batch_request:
        datasource_name: pd_fs_ds
        data_asset_name: monthly_taxi_data
        options: {}
        batch_slice:
        expectation_suite_name: my_other_suite
    profilers: []
    ge_cloud_id:
    expectation_suite_ge_cloud_id:
    ```
    </TabItem>
</Tabs>

##### Case 4: API calls
<Tabs 
   queryString="checkpoint_case_4_api"
   defaultValue="v0_checkpoint_case_4_api"
   values={[
      {value: 'v0_checkpoint_case_4_api', label: 'V0 Checkpoint API'}
   ]}
>
    <TabItem value="v0_checkpoint_case_4_api" label="V0 Checkpoint API">
    ```python
    import great_expectations as gx

    context = gx.get_context(mode="file")

    datasource = context.sources.add_pandas_filesystem(name="pd_fs_ds", base_directory="data")
    monthly = datasource.add_csv_asset(name="monthly_taxi_data", batching_regex=r"sampled_yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})\.csv")

    suite = context.add_expectation_suite(
        expectation_suite_name="my_suite",
        data_asset_type="CSVAsset",
    )
    validator = context.get_validator(batch_request=monthly.build_batch_request(), expectation_suite_name="my_suite")
    validator.expect_column_values_to_be_between(column="passenger_count", min_value=0, max_value=10)
    validator.save_expectation_suite(discard_failed_expectations=False)

    other_suite = context.add_expectation_suite(
        expectation_suite_name="my_other_suite",
        data_asset_type="CSVAsset",
    )
    validator = context.get_validator(batch_request=monthly.build_batch_request(), expectation_suite_name="my_other_suite")
    validator.expect_column_values_to_be_between(column="passenger_count", min_value=0, max_value=4)
    validator.save_expectation_suite(discard_failed_expectations=False)

    batch_request = monthly.build_batch_request()

    email_action_config = {
        "name": "my_email_action",
        "action": {
            "class_name": "EmailAction",
            "notify_on": "all",
            "use_tls": True,
            "use_ssl": False,
            "renderer": {
                "module_name": "great_expectations.render.renderer.email_renderer",
                "class_name": "EmailRenderer"
            },
            "smtp_address": "smtp.myserver.com",
            "smtp_port": 587,
            "sender_login": "sender@myserver.com",
            "sender_password": "XXXXXXXXXX",
            "sender_alias": "alias@myserver.com",
            "receiver_emails": "receiver@myserver.com",
        }
    }

    action_list = [
        {'name': 'store_validation_result',
        'action': {'class_name': 'StoreValidationResultAction'}},
        {'name': 'store_evaluation_params',
        'action': {'class_name': 'StoreEvaluationParametersAction'}},
        {'name': 'update_data_docs',
        'action': {'class_name': 'UpdateDataDocsAction'}},
        email_action_config
    ]

    checkpoint_config = {
        "name": "top_level_and_validation_override_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "module_name": "great_expectations.checkpoint",
        "expectation_suite_name": "my_suite",
        "action_list": action_list,
        "validations": [
            {
                "expectation_suite_name": "my_other_suite",
                "batch_request": batch_request,
            }
        ],
    }

    context.add_checkpoint(**checkpoint_config)
    result = context.run_checkpoint("top_level_and_validation_override_checkpoint")
    ```
    </TabItem>
</Tabs>

### Data Context Variables
The Data Context variables will be automatically converted for GX Cloud users when switching from V0 to V1. For file context users, we will show the difference in the yaml so you can translate the configuration block in `great_expectations.yml`.

<Tabs 
   queryString="data_context_config"
   defaultValue="v0_data_context_config"
   values={[
      {value: 'v0_data_context_config', label: 'V0 Data Context Configuration'},
      {value: 'v1_data_context_config', label: 'V1 Data Context Configuration'}
   ]}
>
    <TabItem value="v0_data_context_config" label="V0 Data Context Configuration">
    ```yaml
    config_version: 3.0
    config_variables_file_path: uncommitted/config_variables.yml
    plugins_directory: plugins/
    stores:
    expectations_store:
        class_name: ExpectationsStore
        store_backend:
        class_name: TupleFilesystemStoreBackend
        base_directory: expectations/
    validations_store:
        class_name: ValidationsStore
        store_backend:
        class_name: TupleFilesystemStoreBackend
        base_directory: uncommitted/validations/
    evaluation_parameter_store:
        class_name: EvaluationParameterStore
    checkpoint_store:
        class_name: CheckpointStore
        store_backend:
        class_name: TupleFilesystemStoreBackend
        suppress_store_backend_id: true
        base_directory: checkpoints/
    profiler_store:
        class_name: ProfilerStore
        store_backend:
        class_name: TupleFilesystemStoreBackend
        suppress_store_backend_id: true
        base_directory: profilers/
    expectations_store_name: expectations_store
    validations_store_name: validations_store
    evaluation_parameter_store_name: evaluation_parameter_store
    checkpoint_store_name: checkpoint_store
    data_docs_sites:
    local_site:
        class_name: SiteBuilder
        show_how_to_buttons: true
        store_backend:
        class_name: TupleFilesystemStoreBackend
        base_directory: uncommitted/data_docs/local_site/
        site_index_builder:
        class_name: DefaultSiteIndexBuilder
    anonymous_usage_statistics:
    data_context_id: a7441dab-9db7-4043-a3e7-011cdab54cfb
    enabled: false
    usage_statistics_url: https://qa.stats.greatexpectations.io/great_expectations/v1/usage_statistics
    fluent_datasources:
    spark_fs:
        type: spark_filesystem
        assets:
        directory_csv_asset:
            type: directory_csv
            data_directory: data
        spark_config:
        spark.executor.memory: 4g
        persist: true
        base_directory: data
    notebooks:
    include_rendered_content:
    globally: false
    expectation_suite: false
    expectation_validation_result: false
    ```
    </TabItem>
    <TabItem value="v1_data_context_config" label="V0 Data Context Configuration">
    ```yaml
    config_version: 4.0
    config_variables_file_path: uncommitted/config_variables.yml
    plugins_directory: plugins/
    stores:
    expectations_store:
        class_name: ExpectationsStore
        store_backend:
        class_name: TupleFilesystemStoreBackend
        base_directory: expectations/
    validation_results_store:
        class_name: ValidationResultsStore
        store_backend:
        class_name: TupleFilesystemStoreBackend
        base_directory: uncommitted/validations/
    checkpoint_store:
        class_name: CheckpointStore
        store_backend:
        class_name: TupleFilesystemStoreBackend
        suppress_store_backend_id: true
        base_directory: checkpoints/
    validation_definition_store:
        class_name: ValidationDefinitionStore
        store_backend:
        class_name: TupleFilesystemStoreBackend
        base_directory: validation_definitions/
    expectations_store_name: expectations_store
    validation_results_store_name: validation_results_store
    checkpoint_store_name: checkpoint_store
    data_docs_sites:
    local_site:
        class_name: SiteBuilder
        show_how_to_buttons: true
        store_backend:
        class_name: TupleFilesystemStoreBackend
        base_directory: uncommitted/data_docs/local_site/
        site_index_builder:
        class_name: DefaultSiteIndexBuilder
    analytics_enabled: true
    fluent_datasources:
    spark_ds:
        type: spark
        id: 134de28d-bfdc-4980-aa2e-4f59788afef3
        assets:
        taxi_dataframe_asset:
            type: dataframe
            id: 4110d2ff-5711-47df-a4be-eaefc2a638b4
            batch_metadata: {}
            batch_definitions:
            taxi_dataframe_batch_def:
                id: 76738b8b-28ab-4857-aa98-f0ff80c8f137
                partitioner:
        spark_config:
        spark.executor.memory: 4g
        force_reuse_spark_context: true
        persist: true
    data_context_id: 12bc94a0-8ac3-4e97-bf90-03cd3d92f8c4
    ```
    </TabItem>
</Tabs>

**config_version**: For V1 this should be set to 4.0

**config_variables_file_path**: This is unchanged.

**plugins_directory**: This is unchanged.

**stores**: This is a dictionary of store names to configuration. In V0 the keys names were configurable. In V1, there is a fixed set of keys. These V1 keys are:

> **expectations_store**: The configuration of the Expectations store. The value here is unchanged from the value that was stored with the key that was configured in the top-level variable **expectations_store_name**.

> **validation_results_store**: The configuration to the validation results store. The value here is slightly changed from the value that was stored with the key that was configured in the top-level variable  validations_store_name. The value change is ValidationsStore is now ValidationResultsStore.

> **checkpoint_store**: This key and value are unchanged between V0 and V1.

> **validation_definition_store**: Validation definitions are a new concept in V1. For file-based contexts, you can use this example V1 configuration directly. You can update the base_directory if you need to change the path where the configuration for validation definitions get stored.

**expectations_store_name**: While still present, this must now always be set to “expectations_store”.

**validations_store_name**: This should now be “validation_results_store_name”. Its value must be the value “validation_results_store“.

**evaluation_parameter_store_name**: This key has been removed. One can no longer store evaluation_parameters since they are now a runtime concept called expectation_parameters. If you want to set a default value of an expectation parameter, you should do that in code where you run the validation.

**checkpoint_store_name**: This parameter name is unchanged. The value must be “checkpoint_store”.

**data_docs_sites**: This key and its value are unchanged in V1.

**anonymous_usage_statistics**: 

> **enabled**: This value is now the top-level key analytics_enabled

> **data_context_id**: This value is now the top-level key data_context_id

> **usage_statistics_url**: This field is no longer configurable.

**fluent_datasources**: While this appears in the great_expectations.yml file, it is not a data context variable. Please see the “Data Sources and Data Assets” portion of this doc for instructions on migrating this from V0 to V1.

**notebooks**: This is no longer supported and does not appear in V1’s configuration.

**include_rendered_content**: This only mattered for GX Cloud users and no longer appears in this configuration file.

##### New V1 Fields

**data_context_id**: If previously one had the field **anonymous_usage_statistics.data_context_id** set, one should use that value here. Otherwise, this can be set to a unique, arbitrary UUID.

##### Store Backends
In previous versions of GX, we supported a number of configurable backend stores, including ones that persisted to databases, S3, Google Cloud Platform, and Azure. V1 drops support for these; file contexts only use `TupleFilesystemStoreBackend` and cloud contexts only use cloud stores. A number of GX users have a need for persisting their configurations, or subsets of their configurations, outside of their filesystem and either cannot or would prefer not to use cloud contexts. While GX no longer supports the tooling for these persistence models directly, users may use external libraries/services to handle this, e.g. copying their config to S3 via boto3.

#### V1 API
In V1, the configuration for all data context variables can be changed via the Python API. For a data context named context one can view via `context.variables.<variable_name>` and update via:

```python
context.variables.<variable_name> = new_value
context.variables.save()

# At this time you need to reload the context to have it take effect
context = gx.get_context() 
```