---
title: How to convert an Ephemeral Data Context to a Filesystem Data Context
tag: [how-to, setup]
keywords: [Great Expectations, Ephemeral Data Context, Filesystem Data Context]
---

import Prerequisites from '/docs/components/_prerequisites.jsx'
import IfYouStillNeedToSetupGx from '/docs/components/prerequisites/_if_you_still_need_to_setup_gx.md'
import ConnectingToDataFluently from '/docs/components/connect_to_data/link_lists/_connecting_to_data_fluently.md'

## Introduction

An Ephemeral Data Context is a temporary, in-memory Data Context that will not persist beyond the current Python session.  However, if you decide you would like to save the contents of an Ephemeral Data Context for future use you can do so by converting it to a Filesystem Data Context.

## Prerequisites

<Prerequisites>

- A working installation of Great Expectations
- An Ephemeral Data Context instance
- A passion for Data Quality

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
- [How to quickly instantiate a Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context.md)

You can also explicitly instantiate an Ephemeral Data Context (for those occasions when your system is set up to work with GX Cloud or you do have a previously initialized Filesystem Data Context).  For more information, see:
- [How to explicitly instantiate an Ephemeral Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_explicitly_instantiate_an_ephemeral_data_context.md)

You can also check to see if a context you are currently using is 

</details>

<details>

<summary>

### If you aren't certain that your Data Context is Ephemeral...

</summary>

You can easily check to see if you are working with an Ephemeral Data Context with the following code (in this example, we are assuming your Data Context is stored in the variable `context`):

```python title="Python code"
from great_expectations.data_context import EphemeralDataContext

# ...

if isinstance(context, EphemeralDataContext):
    print(It's Ephemeral!)
```

</details>

## Steps

### 1. (Optional) Determine where you want to create a new Filesystem Data Context at

The process of converting an Ephemeral Data Context to a Filesystem Data Context creates a **new** Filesystem Data Context.  You cannot append the contents of your Ephemeral Data Context into an existing Filesystem Data Context.  Therefore, it is recommended that you pick an empty folder to initialize the new Filesystem Data Context in.  

```python title="Python code"

```

If you do not specify a location, GX will attempt to initialize your new Filesystem Data Context in the folder that your Python code is running from.

### 2. Convert the Ephemeral Data Context into a Filesystem

Converting an Ephemeral Data Context into a Filesystem Data Context can be done with one line of code:

```python title="Python code"
context = context.convert_to_file_context()
```

## Next steps

### Customizing the configuration of a Data Context

### Connecting GX to source data systems

<ConnectingToDataFluently />