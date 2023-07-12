---
title: Validate Multiple Batches within a Single Checkpoint Using a Batch Request List
---

import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

By following this guide, you can efficiently process multiple batches of data using a simple approach. This method eliminates the need to create multiple versions of checkpoints gand assets, streamlining the process. By adhering to the steps outlined below, you can ensure that all batches from a list stored in the asset are processed, preventing any potential issue of only processing the last batch. 

<Prerequisites>

- [Configure a Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [Create a Datasource](/docs/guides/connecting_to_your_data/connect_to_data_lp).
- [Create an Expectation Suite](docs/guides/expectations/expectations_lp). 

</Prerequisites>

## Create Batch Request with Multiple Batches

:::tip
When working with a Filesystem Data Source and organizing batches, the **batching_regex** argument enables the loading of multiple batches onto a single asset. On the other hand, when utilizing SQL Datasource, the asset generates a single batch by default, which can then be split into multiple batches using splitters.

This SQL-based asset can either be a merged table resulting from multiple tables joined through a SQL query (using the `add_query_asset` function), or an entire table loaded as a single batch using the `add_table_asset` function.
:::

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py build_a_batch_request_with_multiple_batches"
```

## Create Batch Request List

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py add_batch_list"
```
In this scenario, the asset's build batch request combines all the data into a single batch, which is then linked to a corresponding batch request. Following that, in the second line, we utilize the `get_batch_list_from_batch_request` function to divide the single batch into multiple individual batches, forming a list. Moving on to the third line, each split-apart batch is associated with its respective batch request. Ultimately, we compile these batch requests, along with their corresponding batches, into a `batch_request_list`.

### Build a Validations List 

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_validate_multiple_batches_within_single_checkpoint.py add_validations"
```
Next, we initiate the creation of a`validations` object. In this particular instance, we associate each batch request with its corresponding expectation suite. 

## Run Checkpoint

Subsequently, we provide the validations object, containing the pairings of batch requests and expectation suites, to a single checkpoint object. 

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_validate_multiple_batches_within_single_checkpoint.py add_checkpoint"
```

This `checkpoint` object effectively handles and processes all the batch requests and their respective batches.

This approach allows us to streamline the processing of multiple data batches by utilizing just one asset and one checkpoint. Consequently, there is no need to create multiple assets and checkpoints to accomplish this task.