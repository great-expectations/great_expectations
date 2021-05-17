---
title: Datasource configuration details
---

:::warning TODO
:::

:::note What does the configuration contain?
A `Datasource` named `my_postgres_datasource`.

An `ExecutionEngine` with a `connection_string`.

The configuration also contains 2 `DataConnectors` by default:
1. A `RuntimeDataConnector` named `default_runtime_data_connector_name` which loads your data into a Batch, and a default `batch_identifier` which identifies your Batches.
2. A `InferredAssetSqlDataConnector` named `default_inferred_data_connector_name` which allows you to name a `whole_table` to retrieve your Batch.  
:::

:::warning TODO
  - Add blurb about ActiveDataConnectors here
:::

:::warning TODO
  - Add test for yaml here
:::
