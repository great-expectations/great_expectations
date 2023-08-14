---
title: How to connect to data on GCS using Pandas
tag: [how-to, connect to data]
description: A technical guide demonstrating how to connect Great Expectations to dat stored on Google Cloud Storage using Pandas.
keywords: [Great Expectations, Google Cloud Storage, GCS, Pandas]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ### 1. Import GX and instantiate a Data Context -->
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'

<!-- ### 3. Add GCS data to the Data Source as a Data Asset -->
import BatchingRegexExplaination from '/docs/components/connect_to_data/cloud/_batching_regex_explaination.mdx'
import GCSFluentAddDataAssetConfigKeys from '/docs/components/connect_to_data/cloud/_gcs_fluent_data_asset_config_keys.mdx'

<!-- Next steps -->
import AfterCreateNonSqlDatasource from '/docs/components/connect_to_data/next_steps/_after_create_non_sql_datasource.md'

In this guide we will demonstrate how to use Pandas to connect to data stored on Google Cloud Storage.  In our examples, we will specifically be connecting to csv files.  However, Great Expectations supports most types of files that Pandas has read methods for.

## Prerequisites

<Prerequisites>

- [An installation of GX set up to work with GCS](/docs/guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_on_gcs)
- Access to data on a GCS bucket

</Prerequisites> 

## Steps

### 1. Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### 2. Create a Data Source

We can define a GCS datasource by providing three pieces of information:
- `name`: In our example, we will name our Data Source `"my_gcs_datasource"`
- `bucket_or_name`: In this example, we will provide a GCS bucket
- `gcs_options`: We can provide various additional options here, but in this example we will leave this empty and use the default values.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_gcs_using_pandas.py define_add_pandas_gcs_args"
```

Once we have those three elements, we can define our Data Source like so:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_gcs_using_pandas.py create_datasource"
```

### 3. Add GCS data to the Data Source as a Data Asset

<GCSFluentAddDataAssetConfigKeys />

We will define these values and create our DataAsset with the code:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_gcs_using_pandas.py add_asset"
```

<BatchingRegexExplaination storage_location_type="GCS bucket" />

## Next steps

<AfterCreateNonSqlDatasource />

## Additional information

<!-- TODO: Add this once we have a script.
### Code examples

To see the full source code used for the examples in this guide, please reference the following scripts in our GitHub repository:
- [script_name.py](https://path/to/the/script/on/github.com)
-->

<!-- ### GX Python APIs
 
 For more information on the GX Python objects and APIs used in this guide, please reference the following pages of our public API documentation:
 
 - `get_context(...)`
 - `DataContext.datasources.add_pandas_gcs(...)`
 - `Data Source.add_csv_asset(...)` -->

### External APIs

For more information on Google Cloud and authentication, please visit the following:
* [gcloud CLI Tutorial](https://cloud.google.com/storage/docs/reference/libraries)
* [GCS Python API Docs](https://googleapis.dev/python/storage/latest/index.html)

### Related reading

For more details regarding storing credentials for use with GX, please see our guide: [How to configure credentials](/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials)