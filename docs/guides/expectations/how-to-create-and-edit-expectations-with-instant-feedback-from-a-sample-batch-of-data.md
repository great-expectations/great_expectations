---
title: ✳ How to create and edit Expectations with instant feedback from a sample Batch of data
---

import Prerequisites from '../../guides/connecting_to_your_data/components/prerequisites.jsx'

<Prerequisites>

- Configured a [Data Context](../../tutorials/getting-started/initialize-a-data-context.md).

</Prerequisites>

Steps
-----

1. The ``--interactive`` mode denotes the fact that you are interacting with your data.  In
    other words, you have access to a data source and can specify a Batch of data to be used to create Expectations
    against.  ``--manual`` mode
    (please see [How to create and edit Expectations based on domain knowledge, without inspecting data directly](./how-to-create-and-edit-expectations-based-on-domain-knowledge-without-inspecting-data-directly))
    still allows you to create expectations (e.g., if you already know enough about your data, such as the various columns
    in a database table), but you will not be able to run validations, until you specify a Batch of data,
    which can be done at a later point; in fact, you can switch back and forth between the interactive and
    manual modes, and all your expectations will be intact.

   Run this command in the root directory of your project (where the init command created the ``great_expectations`` subdirectory:

   ```bash
   great_expectations --v3-api suite new --interactive
   ```

   This command prompts you to select a datasource, a data connector, and a data asset so as to identify a sample
   Batch of data the suite will eventually describe.  If there are unique choices (e.g., only one data connector in
   your datasource configuration), then Great Expectations will automatically select it for you (to speed up the process).

   Finally, unless you specify the name of the Expectation Suite on the command line (using the
   ``--expectation-suite`` option), the command will ask you to name your new Expectation Suite and offer you a
   default name to simply accept, or provide your own.

   Then an empty suite is created and added to your project.

   Then Great Expectations creates a jupyter notebook for you to start creating your new suite.  The command
   concludes by opening the newly generated jupyter notebook.

2. If you wish to skip the automated opening of jupyter notebook, add the ``--no-jupyter`` flag:

   ```bash
   great_expectations --v3-api suite new --interactive --no-jupyter
   ```

3. One of the easiest ways to get starting in the interactive mode is to take advantage of the `--profile` flag
   (please see [How to create and edit Expectations with a Profiler](./how-to-create-and-edit-expectations-with-a-profiler.md)).

   :::info
   When in the interactive mode, the initialization cell of your jupyter notebook will contain the ``batch_request``
   dictionary.  You can convert it to JSON and save in a file for future use.  The contents of this file would look like this:
   :::

   ```bash
   {
       "datasource_name": my_datasource",
       "data_connector_name": "my_data_connector",
       "data_asset_name": "my_asset"
   }
   ```

   You can then utilize this saved ``batch_request`` (containing any refinements you may have made to it in your notebook)
   and skip the steps of selecting its components:

   ```bash
   great_expectations --v3-api suite new --interactive --batch-request my_saved_batch_request_file.json
   ```

   Unless you specify the name of the Expectation Suite on the command line (using the ``--expectation-suite MY_SUITE`` syntax),
   the command will ask you to name your new Expectation Suite and offer you a default name for you to simply accept, or provide your own.

   You can extend the previous example to specify the name of the Expectation Suite on the command line as follows:

   ```bash
   great_expectations --v3-api suite new --expectation-suite my_suite --interactive --batch-request my_saved_batch_request.json
   ```

   :::info
   To check the syntax, you can always run the following command in the root directory of your project (where the ``init`` command created the ``great_expectations`` subdirectory:
   :::

   ```bash
   great_expectations --v3-api suite new --help
   ```
