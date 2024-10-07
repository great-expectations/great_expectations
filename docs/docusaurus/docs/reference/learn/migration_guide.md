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