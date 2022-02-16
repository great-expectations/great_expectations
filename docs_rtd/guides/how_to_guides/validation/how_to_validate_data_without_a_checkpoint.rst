.. _how_to_guides__validation__how_to_validate_data_without_a_checkpoint:

How to validate data without a Checkpoint
=========================================

This guide demonstrates how to load an Expectation Suite and validate data without using a :ref:`Checkpoint<how_to_guides__validation__how_to_create_a_new_checkpoint>`. This might be suitable for environments or workflows where a user does not want to or cannot create a Checkpoint, e.g. in a :ref:`hosted environment<deployment_hosted_enviroments>`.


.. content-tabs::

    .. tab-container:: tab0
        :title: Docs for Legacy Checkpoints (<=0.13.7)


        .. admonition:: Prerequisites: This how-to guide assumes you have already:

          - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
          - :ref:`Created an Expectation Suite <how_to_guides__creating_and_editing_expectations>`

        The following code mirrors the code provided in the ``validation_playground.ipynb`` notebooks in ``great_expectations/notebooks``. First of all, we import Great Expectations, load our :ref:`Data Context<data_context>`, and define variables for the Datasource we want to access:

        .. code-block:: python

            import great_expectations as ge
            context = ge.data_context.DataContext()

            datasource_name = "my_datasource"

        We then create a Batch using the above arguments. The ``batch_kwargs`` differ based on the type of data asset you want to connect to. The first example demonstrates the different possible ``batch_kwargs`` you could use to define your data for a SQLAlchemy Datasource:

        .. code-block:: python

            # If you would like to validate an entire table or view in your database's default schema:
            batch_kwargs = {'table': "YOUR_TABLE", 'datasource': datasource_name}

            # If you would like to validate an entire table or view from a non-default schema in your database:
            batch_kwargs = {'table': "YOUR_TABLE", "schema": "YOUR_SCHEMA", 'datasource': datasource_name}

            # If you would like to validate the result set of a query:
            batch_kwargs = {'query': 'SELECT YOUR_ROWS FROM YOUR_TABLE', 'datasource': datasource_name}

        The following ``batch_kwargs`` can be used to create a batch for a Pandas or PySpark Datasource:

        .. code-block:: python

            # If you would like to validate a file on a filesystem:
            batch_kwargs = {'path': "YOUR_FILE_PATH", 'datasource': datasource_name}

            # If you would like to validate in a PySpark or Pandas dataframe:
            batch_kwargs = {'dataset': "YOUR_DATAFRAME", 'datasource': datasource_name}

        Finally, we create the batch using those ``batch_kwargs`` and the name of the Expectation Suite we want to use, and run validation:

        .. code-block:: python

            batch = context.get_batch(batch_kwargs, "my_expectation_suite")

            results = context.run_validation_operator(
                "action_list_operator",
                assets_to_validate=[batch],
                run_id="my_run_id") # Make my_run_id a unique identifier, e.g. a timestamp


        This runs validation and executes any ValidationActions configured for this ValidationOperator (e.g. saving the results to a ValidationResult Store).

    .. tab-container:: tab1
        :title: Docs for Class-Based Checkpoints (>=0.13.8)

        .. Attention:: As part of the new modular expectations API in Great Expectations, Validation Operators have evolved into Class-Based Checkpoints. This means running a Validation without a Checkpoint is no longer supported in Great Expectations version 0.13.8 or later. Please read :ref:`Checkpoints and Actions<checkpoints_and_actions>` to learn more.



.. discourse::
    :topic_identifier: 229
