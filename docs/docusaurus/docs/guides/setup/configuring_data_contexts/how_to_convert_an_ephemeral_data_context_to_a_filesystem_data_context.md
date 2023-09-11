---
title: Convert an Ephemeral Data Context to a Filesystem Data Context
tag: [how-to, setup]
keywords: [Great Expectations, Ephemeral Data Context, Filesystem Data Context]
---

import Prerequisites from '/docs/components/_prerequisites.jsx'
import IfYouStillNeedToSetupGx from '/docs/components/prerequisites/_if_you_still_need_to_setup_gx.md'
import ConnectingToDataFluently from '/docs/components/connect_to_data/link_lists/_connecting_to_data_fluently.md'
import SetupConfigurations from '/docs/components/setup/link_lists/_setup_configurations.md'

An Ephemeral Data Context is a temporary, in-memory Data Context that will not persist beyond the current Python session.  However, if you decide you would like to save the contents of an Ephemeral Data Context for future use you can do so by converting it to a Filesystem Data Context.

## Prerequisites

<Prerequisites>

- A working installation of Great Expectations
- An Ephemeral Data Context instance

</Prerequisites> 


## Confirm your Data Context is Ephemeral

To confirm that you're working with an Ephemeral Data Context, run the following code:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_explicitly_instantiate_an_ephemeral_data_context.py check_data_context_is_ephemeral"
```

In the example code, it is assumed that your Data Context is stored in the variable `context`.

## Verify that your current working directory does not already contain a GX Filesystem Data Context

The method for converting an Ephemeral Data Context to a Filesystem Data Context initializes the new Filesystem Data Context in the current working directory of the Python process that is being executed.  If a Filesystem Data Context already exists at that location, the process will fail.

You can determine if your current working directory already has a Filesystem Data Context by looking for a `great_expectations.yml` file.  The presence of that file indicates that a Filesystem Data Context has already been initialized in the corresponding directory.

## Convert the Ephemeral Data Context into a Filesystem Data Context

Converting an Ephemeral Data Context into a Filesystem Data Context can be done with one line of code:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_explicitly_instantiate_an_ephemeral_data_context.py convert_ephemeral_data_context_filesystem_data_context"
```

:::info Replacing the Ephemeral Data Context

The `convert_to_file_context()` method does not change the Ephemeral Data Context itself.  Rather, it initializes a new Filesystem Data Context with the contents of the Ephemeral Data Context and then returns an instance of the new Filesystem Data Context.  If you do not replace the Ephemeral Data Context instance with the Filesystem Data Context instance, it will be possible for you to continue using the Ephemeral Data Context.  

If you do this, it is important to note that changes to the Ephemeral Data Context **will not be reflected** in the Filesystem Data Context.  Moreover, `convert_to_file_context()` does not support merge operations. This means you will not be able to save any additional changes you have made to the content of the Ephemeral Data Context.  Neither will you be able to use `convert_to_file_context()` to replace the Filesystem Data Context you had previously created: `convert_to_file_context()` will fail if a Filesystem Data Context already exists in the current working directory.

For these reasons, it is strongly advised that once you have converted your Ephemeral Data Context to a Filesystem Data Context you cease working with the Ephemeral Data Context instance and begin working with the Filesystem Data Context instance instead.

:::

## Next steps

- [Configure credentials](/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials)
- [Configure Expectation Stores](/docs/guides/setup/configuring_metadata_stores/configure_expectation_stores)
- [Configure Validation Result Stores](/docs/guides/setup/configuring_metadata_stores/configure_result_stores)
- [Configure a MetricStore](/docs/guides/setup/configuring_metadata_stores/how_to_configure_a_metricsstore)
- [Host and share Data Docs](/docs/guides/setup/configuring_data_docs/host_and_share_data_docs)
- [Connect to filesystem source data](/docs/guides/connecting_to_your_data/fluent/filesystem/connect_filesystem_source_data)
- [Connect to in-memory source data](/docs/guides/connecting_to_your_data/fluent/in_memory/connect_in_memory_data)
- [Connect to SQL database source data](/docs/guides/connecting_to_your_data/fluent/database/connect_sql_source_data)