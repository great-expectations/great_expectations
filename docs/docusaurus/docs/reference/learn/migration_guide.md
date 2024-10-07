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

### Configuration file differences

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

    **expectation_type**: This has been changed to type.

    **kwargs**: This is unchanged

    **meta**: This dictionary that a user can populate with whatever metadata they would like. The notes key that Great Expectations Cloud used has been pulled out into a top level key.

    **id**: This new field introduced in 1.0 can be any arbitrary, unique UUID. When migrating, generate and add a UUID.

    **notes (new field)**: This new top-level field replaces meta.notes. This is consumed by Great Expectations Cloud to display user notes on the Cloud UI.

**ge_cloud_id**: This is now id. This is now a required field. Migrators can generate a unique, arbitrary UUID and add it.

**meta**: The format is unchanged.

**notes**: This is new in 1.0 and is an arbitrary string.