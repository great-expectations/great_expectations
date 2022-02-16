.. _how_to_guides__creating_and_editing_expectations__how_to_create_a_new_expectation_suite_using_the_cli:

How to create a new Expectation Suite using the CLI
***************************************************

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        While you could hand-author an Expectation Suite by writing a JSON file, just like with other features it is easier to let the :ref:`CLI <command_line>` save you time and typos.
        Run this command in the root directory of your project (where the init command created the ``great_expectations`` subdirectory):


        .. code-block:: bash

            great_expectations suite new


        This command prompts you to name your new Expectation Suite and to select a sample Batch of data that the suite will eventually describe.
        An empty suite is created and added to your project.
        Then it creates a jupyter notebook for you to start creating your new suite.
        The command concludes by opening the newly generated jupyter notebook.

        If you wish to skip the automated opening of jupyter notebook, add the `--no-jupyter` flag:


        .. code-block:: bash

            great_expectations suite new --no-jupyter


    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        While you could hand-author an Expectation Suite by writing a JSON file, just like with other features it is easier to let the :ref:`CLI <command_line>` save you time and typos.

        Simply run this command and follow the prompts:

        .. code-block:: bash

            great_expectations --v3-api suite new

        Alternatively, if you wish to skip some steps by providing flags please follow the rest of this document. We will walk through several available options for how to do this.

        1. The ``--interactive`` mode denotes the fact that you are interacting with your data.  In
        other words, you have access to a data source and can specify a Batch of data to be used to create Expectations
        against.  ``--manual`` mode still allows you to create expectations (e.g., if you already know enough
        about your data, such as the various columns in a database table), but you will not be able to run validations,
        until you specify a Batch of data, which can be done at a later point; in fact, you can switch back and forth
        between the interactive and manual modes, and all your expectations will be intact.

        Run this command in the root directory of your project (where the init command created the ``great_expectations`` subdirectory:

        .. code-block:: bash

            great_expectations --v3-api suite new --interactive

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

        .. code-block:: bash

            great_expectations --v3-api suite new --interactive --no-jupyter

        or in the manual mode:

        .. code-block:: bash

            great_expectations --v3-api suite new --manual --no-jupyter


        3. One of the easiest ways to get starting in the interactive mode is to take advantage of the `--profile` flag:

        .. code-block:: bash

            great_expectations --v3-api suite new --interactive --profile

        This instructs Great Expectations to inspect your Batch of data and suggest the initial set of Expectations.
        When the notebook opens (or when you open it later if the above command is used with the ``--no-jupyter`` flag),
        you can edit these expectations.  For example, you might tighten some tolerances, compared to the initial values,
        based on your knowledge of your dataset.  After that, you can review and save your Expectation Suite and run validations using it.

        Here are some general guidelines for taking advantage of the profiling capability for bootstrapping your Expectation Suite.

        Within the notebook, run the first cell in the notebook that loads the data.  (This step was explained above.)

        The next code cell in the notebook presents you with a list of all the columns found in your selected data:

        .. code-block:: python

            ignored_columns = [
                "Name",
                "Age",
                "Address",
                "Occupation",
                ...
            ]

        By default, all columns are ignored. To select which columns you want to be profiled for generating Expectations on, simply comment them out to include them.

        The next code cell is where you will configure and instantiate your profiler, and build your suite. You can leave these defaults as-is for now - :ref:`learn more about the available parameters here. <how_to_guides__creating_and_editing_expectations__how_to_create_an_expectation_suite_with_the_user_configurable_profiler>`

        When you run this cell and build your suite, you will see a list of the expectations included by column. At this point, you may also make modifications to the ignored_columns or the profiler, and re-run the cell.

        .. code-block:: python

            profiler = UserConfigurableProfiler(
                profile_dataset=validator,
                excluded_expectations=None,
                ignored_columns=ignored_columns,
                not_null_only=False,
                primary_or_compound_key=False,
                semantic_types_dict=None,
                table_expectations_only=False,
                value_set_threshold="MANY",
            )
            suite = profiler.build_suite()

        Finally, run the next few code cells to see the automatically generated Expectation Suite in Data Docs.

        Because the profiler-based expectations are too permissive (i.e., lax tolerances), you will want to edit this
        suite to tune the parameters and make any adjustments such as removing :ref:`Expectations` that don't make sense
        for your use case. You can iterate on included and excluded columns and Expectations to get closer to the Expectation Suite you want.

        .. important::

            The Suites generated by the profiler **are not meant to be production suites** -- they are the initial estimates to build upon.

            Great Expectations will choose which expected values for Expectations **might make sense** for a column based on the type and cardinality of the data in each selected column.

            You will definitely want to edit the Suite to fine-tune it after auto-generating it with the ``--profile`` flag.

        When in the interactive mode, the initialization cell of your jupyter notebook will contain the ``batch_request``
        dictionary.  You can convert it to JSON and save in a file for future use.  The contents of this file would look like this:

        .. code-block:: bash

            {
                "datasource_name": my_datasource",
                "data_connector_name": "my_data_connector",
                "data_asset_name": "my_asset"
            }

        You can then utilize this saved ``batch_request`` (containing any refinements you may have made to it in your notebook)
        and skip the steps of selecting its components:

        .. code-block:: bash

            great_expectations --v3-api suite new --interactive --batch-request my_saved_batch_request_file.json

        Unless you specify the name of the Expectation Suite on the command line (using the ``--expectation-suite MY_SUITE`` syntax),
        the command will ask you to name your new Expectation Suite and offer you a default name for you to simply accept, or provide your own.

        You can extend the previous example to specify the name of the Expectation Suite on the command line as follows:

        .. code-block:: bash

            great_expectations --v3-api suite new --expectation-suite my_suite --interactive --batch-request my_saved_batch_request.json

        To check the syntax, you can always run the following command in the root directory of your project (where the ``init`` command created the ``great_expectations`` subdirectory:

        .. code-block:: bash

            great_expectations --v3-api suite new --help


.. discourse::
    :topic_identifier: 240
