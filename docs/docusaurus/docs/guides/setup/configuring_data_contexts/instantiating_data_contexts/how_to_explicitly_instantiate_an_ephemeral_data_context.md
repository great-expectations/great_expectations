---
title: How to instantiate an Ephemeral Data Context
tag: [how-to, setup]
keywords: [Great Expectations, Ephemeral Data Context]
---

import Prerequisites from '/docs/components/_prerequisites.jsx'
import IfYouStillNeedToSetupGx from '/docs/components/prerequisites/_if_you_still_need_to_setup_gx.md'
import ConnectingToDataFluently from '/docs/components/connect_to_data/link_lists/_connecting_to_data_fluently.md'
import AdmonitionConvertToFileContext from '/docs/components/setup/data_context/_admonition_convert_to_file_context.md'

An Ephemeral Data Context is a temporary, in-memory Data Context.  They are ideal for doing data exploration and initial analysis when you do not want to save anything to an existing project, or for when you need to work in a hosted environment such as an EMR Spark Cluster.

## Prerequisites

<Prerequisites>

- A Great Expectations instance. See [Setup: Overview](../../setup_overview.md).

</Prerequisites> 

## Steps

### 1. Import necessary classes for instantiating an Ephemeral Data Context

To create our Data Context, we will create a configuration that uses in-memory Metadata Stores.  This will require two classes from the Great Expectations module: the `DataContextConfig` class and the `InMemoryStoreBackendDefaults` class.  These can be imported with the code:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_explicitly_instantiate_an_ephemeral_data_context.py import_data_context_config_with_in_memory_store_backend"
```

We will also need to import the `EphemeralDataContext` class that we will be creating an instance of:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_explicitly_instantiate_an_ephemeral_data_context.py import_ephemeral_data_context"
```

### 2. Create the Data Context configuration

To create a Data Context configuration that specifies the use of in-memory Metadata Stores we will pass in an instance of the `InMemoryStoreBackendDefaults` class as a parameter when initializing an instance of the `DataContextConfig` class:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_explicitly_instantiate_an_ephemeral_data_context.py instantiate_data_context_config_with_in_memory_store_backend"
```

### 3. Instantiate an Ephemeral Data Context

To create our Ephemeral Data Context instance, we initialize the `EphemeralDataContext` class while passing in the `DataContextConfig` instance we previously created as the value of the `project_config` parameter.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_explicitly_instantiate_an_ephemeral_data_context.py instantiate_ephemeral_data_context"
```

We now have an Ephemeral Data Context to use for the rest of this Python session.

:::info Saving the contents of an Ephemeral Data Context for future use

<AdmonitionConvertToFileContext />

:::

## Next steps

### Connecting GX to source data systems

Now that you have an Ephemeral Data Context you will want to connect GX to your data.  For this, please see the appropriate guides from the following:

<ConnectingToDataFluently />

### Preserving the contents of an Ephemeral Data Context

An Ephemeral Data Context is a temporary, in-memory object.  It will not persist beyond the current Python session.  If you decide that you would like to keep the contents of your Ephemeral Data Context for future use, please see:

- [How to convert an Ephemeral Data Context to a Filesystem Data Context](/docs/guides/setup/configuring_data_contexts/how_to_convert_an_ephemeral_data_context_to_a_filesystem_data_context)