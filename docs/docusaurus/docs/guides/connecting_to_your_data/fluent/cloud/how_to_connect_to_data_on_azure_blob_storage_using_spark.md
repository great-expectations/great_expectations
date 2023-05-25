---
title: How to connect to data on Azure Blob Storage using Spark
tag: [how-to, connect to data]
description: A brief how-to guide covering ...
keywords: [Great Expectations, Azure Blob Storage, Spark]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'
import PrereqInstallGxWithDependencies from '/docs/components/prerequisites/_gx_installed_with_abs_dependencies.md'

import Introduction from '/docs/components/connect_to_data/intros/_abs_pandas_or_spark.mdx'

<!-- ### 1. Import GX and instantiate a Data Context -->
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'

import AbsFluentAddDataAssetConfigKeys from '/docs/components/connect_to_data/cloud/_abs_fluent_data_asset_config_keys.mdx'
import AbsBatchingRegexExample from '/docs/components/connect_to_data/cloud/_abs_batching_regex_explaination.md'

<!-- ## Next steps -->
import AfterCreateNonSqlDatasource from '/docs/components/connect_to_data/next_steps/_after_create_non_sql_datasource.md'

<Introduction execution_engine='Spark' />

## Prerequisites

<Prerequisites>

- <PrereqInstallGxWithDependencies />
- Access to data in Azure Blob Storage

</Prerequisites> 

## Steps

### 1. Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />


### 2. Create a Datasource

We can define an Azure Blob Storage datasource by providing these pieces of information:
- `name`: In our example, we will name our Datasource `"my_datasource"`
- `azure_options`: We provide authentication settings here

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_azure_blob_storage_using_spark.py define_add_spark_abs_args"
```
We can create a Datasource that points to our Azure Blob Storage with the code:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_azure_blob_storage_using_spark.py create_datasource"
```

:::tip Where did that connection string come from?
In the above example, the value for `account_url` will be substituted for the contents of the `AZURE_STORAGE_CONNECTION_STRING` key you configured when you [installed GX and set up your Azure Blob Storage dependancies](/docs/guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_in_abs).
:::

### 3. Add ABS data to the Datasource as a Data Asset


<AbsFluentAddDataAssetConfigKeys />

Once these values have been defined, we will create our DataAsset with the code:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_azure_blob_storage_using_spark.py add_asset"
```

<AbsBatchingRegexExample />

## Next steps

<AfterCreateNonSqlDatasource />

## Additional information

### Related reading

For more details regarding storing credentials for use with GX, please see our guide: [How to configure credentials](/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials)