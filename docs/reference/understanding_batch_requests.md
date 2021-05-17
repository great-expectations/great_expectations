---
title: Understanding BatchRequests
---

:::warning TODO
stub
:::

:::note What does the RuntimeBatchRequest contain?
1. `datasource_name` and `data_connector_name` are directly from our `Datasource` configuration.
2. `query` is passed in as a `runtime_parameter`, and is used to select 10 rows from table `taxi_data`.

**Note** : Make `data_asset_name` and `batch_identifiers` default.
:::

:::note What does the BatchRequest contain?
1. `datasource_name` and `data_connector_name` are directly from our `Datasource` configuration.
2.  `data_asset_name` is `taxi_data`, which is the name of the table you want to retrieve as a batch.  
The reason you can do this is because of the `InferredAssetDataConnector` that is configured to retrieve `whole_table` by default.

**To Discuss**: How much do we mention `ActiveDataConnector` here?
:::
