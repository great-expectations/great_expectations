---
title: How to connect to data on S3 using Spark
tag: [how-to, connect to data]
description: A technical guide demonstrating how to connect Great Expectations to dat stored on a Amazon Web Services S3 bucket using Spark.
keywords: [Great Expectations, Amazon Web Services, AWS S3, Spark]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ### 1. Import GX and instantiate a Data Context -->
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'

<!-- ### 3. Add S3 data to the Data Source as a Data Asset -->
import BatchingRegexExplaination from '/docs/components/connect_to_data/cloud/_batching_regex_explaination.mdx'
import S3FluentAddDataAssetConfigKeys from '/docs/components/connect_to_data/cloud/_s3_fluent_data_asset_config_keys.mdx'

<!-- Next steps -->
import AfterCreateNonSqlDatasource from '/docs/components/connect_to_data/next_steps/_after_create_non_sql_datasource.md'

In this guide we will demonstrate how to use Spark to connect to data stored on AWS S3.  In our examples, we will specifically be connecting to csv files.

## Prerequisites

<Prerequisites>


- [An installation of GX set up to work with S3](/docs/guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_on_aws_s3)
- Access to data on a S3 bucket

</Prerequisites> 

## Steps

### 1. Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### 2. Create a Data Source

We can define a S3 datasource by providing three pieces of information:
- `name`: In our example, we will name our Data Source `"my_s3_datasource"`
- `bucket_name`: The name of our S3 bucket
- `boto3_options`: We can provide various additional options here, but in this example we will leave this empty and use the default values.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_s3_using_spark.py define_add_spark_s3_args"
```

:::tip What can `boto3_options` specify?

The parameter `boto3_options` will allow you to pass such things as:
- `endpoint_url`: specifies an S3 endpoint.  You can use an environment variable such as `"${S3_ENDPOINT}"` to securely include this in your code.  The string `"${S3_ENDPOINT}"` will be replaced with the value of the corresponding environment variable.
- `region_name`: Your AWS region name.

:::

Once we have those three elements, we can define our Data Source like so:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_s3_using_spark.py create_datasource"
```

### 3. Add S3 data to the Data Source as a Data Asset

<S3FluentAddDataAssetConfigKeys />

We will define these values and create our DataAsset with the code:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_s3_using_spark.py add_asset"
```

<BatchingRegexExplaination storage_location_type="S3 bucket" />

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
 - `DataContext.datasources.add_spark_s3(...)`
 - `Data Source.add_csv_asset(...)` -->

### Related reading

For more details regarding storing credentials for use with GX, please see our guide: [How to configure credentials](/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials)