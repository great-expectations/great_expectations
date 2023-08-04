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

<details>
<summary>

### If you still need to set up and install GX...

</summary>

<IfYouStillNeedToSetupGx />

</details>


<details>
<summary>

### If you still need to create a Data Context...

</summary>

The `get_context()` method will return an Ephemeral Data Context if your system is not set up to work with GX Cloud and a Filesystem Data Context cannot be found.  For more information, see:
- [How to quickly instantiate a Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context)

You can also instantiate an Ephemeral Data Context (for those occasions when your system is set up to work with GX Cloud or you do have a previously initialized Filesystem Data Context).  For more information, see:
- [How to instantiate an Ephemeral Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/instantiate_data_context)

</details>

<details>

<summary>

### If you aren't certain that your Data Context is Ephemeral...

</summary>

You can easily check to see if you are working with an Ephemeral Data Context with the following code (in this example, we are assuming your Data Context is stored in the variable `context`):

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_explicitly_instantiate_an_ephemeral_data_context.py check_data_context_is_ephemeral"
```

</details>

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

### Customize Data Context configurations 

<SetupConfigurations />

### Connect GX to source data systems

<ConnectingToDataFluently />