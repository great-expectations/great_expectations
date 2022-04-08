---
title: How to create and edit Expectations with instant feedback from a sample Batch of data
---

import Prerequisites from '../../guides/connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

<Prerequisites>

- Configured a [Data Context](../../tutorials/getting_started/initialize_a_data_context.md).

</Prerequisites>

## Steps

### 1. Use the CLI to begin the interactive process of creating Expectations 

The ``--interactive`` mode denotes the fact that you are interacting with your data.  In other words, you have access to a <TechnicalTag tag="datasource" text="Datasource" /> and can specify a <TechnicalTag tag="batch" text="Batch" /> of data to be used to create <TechnicalTag tag="expectation" text="Expectations" /> against.  ``--manual`` mode (please see [How to create and edit Expectations based on domain knowledge, without inspecting data directly](./how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly.md)) still allows you to create Expectations (e.g., if you already know enough about your data, such as the various columns in a database table), but you will not be able to <TechnicalTag tag="validation" text="Validate" /> data until you specify a Batch of data, which can be done at a later point; in fact, you can switch back and forth between the interactive and manual modes, and all your Expectations will be intact.

Run this command in the root directory of your project (where the init command created the ``great_expectations`` subdirectory:

```bash
great_expectations suite new --interactive
```

This command prompts you to select a Datasource, a <TechnicalTag tag="data_connector" text="Data Connector" />, and a <TechnicalTag tag="data_asset" text="Data Asset" /> so as to identify a sample Batch of data the <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> will eventually describe.  If there are unique choices (e.g., only one Data Connector in your Datasource configuration), then Great Expectations will automatically select it for you (to speed up the process).

Finally, unless you specify the name of the Expectation Suite on the command line (using the``--expectation-suite`` option), the command will ask you to name your new Expectation Suite and offer you a default name to simply accept, or provide your own.

Then an empty suite is created and added to your project.

Then Great Expectations creates a Jupyter Notebook for you to start creating your new suite.  The command concludes by opening the newly generated Jupyter Notebook.

### 2. (Optional) If you wish to skip the automated opening of Jupyter Notebook, add the ``--no-jupyter`` flag

```bash
great_expectations suite new --interactive --no-jupyter
```

### 3. (Optional) Use the `--profile` CLI flag to assist creating your expectations in the interactive mode. 

One of the easiest ways to get starting in the interactive mode is to take advantage of the `--profile` flag (please see [How to create and edit Expectations with a Profiler](./how_to_create_and_edit_expectations_with_a_profiler.md)).

:::info
When in the interactive mode, the initialization cell of your Jupyter Notebook will contain the ``batch_request``
dictionary.  You can convert it to JSON and save in a file for future use.  The contents of this file would look like this:
:::

```bash
{
    "datasource_name": "my_datasource",
    "data_connector_name": "my_data_connector",
    "data_asset_name": "my_asset"
}
```

You can then utilize this saved ``batch_request`` (containing any refinements you may have made to it in your notebook) and skip the steps of selecting its components:

```bash
great_expectations suite new --interactive --batch-request my_saved_batch_request_file.json
```

Unless you specify the name of the Expectation Suite on the command line (using the ``--expectation_suite MY_SUITE`` syntax),
the command will ask you to name your new Expectation Suite and offer you a default name for you to simply accept, or provide your own.

You can extend the previous example to specify the name of the Expectation Suite on the command line as follows:

```bash
great_expectations suite new --expectation-suite my_suite --interactive --batch-request my_saved_batch_request.json
```

:::info
To check the syntax, you can always run the following command in the root directory of your project (where the ``init`` command created the ``great_expectations`` subdirectory:
:::

```bash
great_expectations suite new --help
```
