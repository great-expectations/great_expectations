.. _how_to_guides__creating_and_editing_expectations__how_to_create_a_new_expectation_suite_using_the_cli:

How to create a new Expectation Suite using the CLI
***************************************************

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        While you could hand-author an Expectation Suite by writing a JSON file, just like with other features it is easier to let :ref:`CLI <command_line>` save you time and typos.
        Run this command in the root directory of your project (where the init command created the ``great_expectations`` subdirectory:


        .. code-block:: bash

            great_expectations suite new


        This command prompts you to name your new Expectation Suite and to select a sample Batch of data the suite will eventually describe.
        Then an empty suite is created and added to your project.
        Then it creates a jupyter notebook for you to start creating your new suite.
        The command concludes by opening the newly generated jupyter notebook.

        If you wish to skip the automated opening of jupyter notebook, add the `--no-jupyter` flag:


        .. code-block:: bash

            great_expectations suite new --no-jupyter


    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        While you could hand-author an Expectation Suite by writing a JSON file, just like with other features it is easier to let :ref:`CLI <command_line>` save you time and typos.

        We will walk through several available options for how to do this.

        The `--interactive` mode (`False` by default) denotes the fact that you are interacting with your data.  In
        other words, you have access to a data source and can specify  a Batch of data to be used to create expectations
        against.  Not specifying this flag still allows you to create expectations (e.g., if you already know enough
        about your data, such as the various columns in a database table), but you will not be able to run validations,
        until you specify a Batch of data, which can be done at a later point; in fact, you can switch back and forth
        between the interactive and non-interactive modes, and all your expectations will be intact.

        Run this command in the root directory of your project (where the init command created the ``great_expectations`` subdirectory:

        .. code-block:: bash

            great_expectations --v3-api suite new --interactive

        This command prompts you to select a datasource, a data connector, and a data asset so as to identify a sample
        Batch of data the suite will eventually describe.  If there are unique choices (e.g., only one data connector in
        your datasource configuration), then Great Expectations will automatically select it for you (to speed up the process).

        Finally, unless you specify the name of the Expectation Suite on the command line (using the `--expectation-suite TEXT` syntax),
        the command will ask you to name your new Expectation Suite and offer you a default name for you to simply accept, or provide your own.

        Then an empty suite is created and added to your project.

        Then Great Expectations creates a jupyter notebook for you to start creating your new suite.  The command
        concludes by opening the newly generated jupyter notebook.

        If you wish to skip the automated opening of jupyter notebook, add the `--no-jupyter` flag:

        .. code-block:: bash

            great_expectations --v3-api suite new --interactive --no-jupyter

        or in the non-interactive mode:

        .. code-block:: bash

            great_expectations --v3-api suite new --no-jupyter


        When in the interactive mode, the initialization cell of your jupyter notebook will contain the `batch_request`
        dictionary.  You can convert it to JSON and save in a file for future use.  The contents of this file would look like this:

        .. code-block:: bash

            {
                "datasource_name": my_datasource",
                "data_connector_name": "my_data_connector",
                "data_asset_name": "my_asset"
            }

        You can then utilize this saved `batch_request` (containing any refinements you may have made to it in your notebook)
        and skip the steps of selecting its components:

        .. code-block:: bash

            great_expectations --v3-api suite new --interactive --batch-request my_saved_batch_request.json

        Unless you specify the name of the Expectation Suite on the command line (using the `--expectation-suite TEXT` syntax),
        the command will ask you to name your new Expectation Suite and offer you a default name for you to simply accept, or provide your own.

        You can extend the previous example to specify the name of the Expectation Suite on the command line as follows:

        .. code-block:: bash

            great_expectations --v3-api suite new --expectation-suite my_suite --interactive --batch-request my_saved_batch_request.json

        You can always run the following command in the root directory of your project (where the init command created the ``great_expectations`` subdirectory to check the syntax:

        .. code-block:: bash

            great_expectations --v3-api suite new --help


.. discourse::
    :topic_identifier: 240
