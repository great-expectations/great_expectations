---
title: How to connect to data on Azure Blob Storage using Pandas
tag: [how-to, connect to data]
description: A brief how-to guide covering ...
keywords: [Great Expectations, Azure Blob Storage, Pandas]
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


<Introduction execution_engine='Pandas' />

## Prerequisites

<Prerequisites>

- <PrereqInstallGxWithDependencies />
- Access to data in Azure Blob Storage

</Prerequisites> 

## Steps

### 1. Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />


### 2. Create a Datasource

We can create a Datasource that points to our Azure Blob Storage with the code:

```python Python code
datasource_name = "my_datasource"
datasource = context.sources.add_pandas_abs(
    name=datasource_name,
    azure_options={
        "account_url": "${AZURE_STORAGE_CONNECTION_STRING}",
    },
)
```

:::tip Where did that connection string come from?
In the above example, the value for `account_url` will be substituted for the contents of the `AZURE_STORAGE_CONNECTION_STRING` key you configured when you [installed GX and set up your Azure Blob Storage dependancies](/docs/guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_in_abs).
:::

### 3. Add ABS data to the Datasource as a Data Asset

<AbsFluentAddDataAssetConfigKeys />

Once these values have been defined, we will define our Data Asset with the code:

```python title="Python code"
data_asset = datasource.add_csv_asset(
    name=asset_name,
    batching_regex=batching_regex,
    abs_container=abs_container,
    abs_name_starts_with=abs_name_starts_with,
)
```

<AbsBatchingRegexExample />

## Next steps

<AfterCreateNonSqlDatasource />

## Additional information

### Related reading

For more details regarding storing credentials for use with GX, please see our guide: [How to configure credentials](/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials)