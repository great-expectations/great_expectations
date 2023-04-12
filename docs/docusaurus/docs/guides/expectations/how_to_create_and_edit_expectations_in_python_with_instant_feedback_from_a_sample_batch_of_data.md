---
title: How to create Expectations interactively in Python
tag: [how-to, getting started]
description: A how-to guide describing how to create Expectations in a Python interpreter or script while interactively receiving feedback by validating them against a Batch of data.
keywords: [Expectations, Interactive Mode, Interactive]
---

import Prerequisites from '/docs/components/_prerequisites.jsx'
import PrerequisiteQuickstartGuideComplete from '/docs/components/prerequisites/_quickstart_completed.mdx'
import IfYouStillNeedToSetupGX from '/docs/components/prerequisites/_if_you_still_need_to_setup_gx.md'
import DataContextInitializeQuickOrFilesystem from '/docs/components/setup/link_lists/_data_context_initialize_quick_or_filesystem.mdx'
import ConnectingToDataFluently from '/docs/components/connect_to_data/link_lists/_connecting_to_data_fluently.md'

In order to Validate data, we must first define a set of Expectations for that data to be validated against.  In this guide, we will walk you through processes of interactively creating and editing Expectations while simultaneously validating them against a Batch of data. By validating our Expectations as we define them we can quickly see if the Expectations we are setting are suitable for our data, and adjust them if necessary.

:::tip Does this process edit my data?
Not at all.  We call this process the "Interactive method" of creating and editing Expectations because validating the Expectations as we work requires interacting with a Batch of data.  This data is introspected to see if it fits the defined Expectation, but it is not edited: only examined.
:::

## Prerequisites

<Prerequisites>

- Great Expectations installed in a Python environment
- Initialized a Filesystem Data Context to save your Expectations in
- Created a Datasource from which to request a Batch of data for introspection
- A passion for data quality

</Prerequisites> 

<details>
<summary>

### If you still need to set up Great Expectations...

</summary>

<IfYouStillNeedToSetupGX />

</details>

<details>
<summary>

### If you still need to initialize your Data Context...

</summary>

Please see the appropriate guide from the following:

<DataContextInitializeQuickOrFilesystem />

</details>

<details>
<summary>

### If you still need to create a Datasource...

</summary>

Please reference the appropriate guide from the following:

<ConnectingToDataFluently />

</details>

## Steps

### 1. Import the Great Expectations module and instantiate a Data Context

### 2. Retrieve a Data Asset from a Datasource in the Data Context

```python
my_asset = context.get_datasource("my_datasource").get_asset("my_taxi_data_asset")
```

### 3. Create a Batch Request

```python
my_batch_request = my_asset.build_batch_request()
```

