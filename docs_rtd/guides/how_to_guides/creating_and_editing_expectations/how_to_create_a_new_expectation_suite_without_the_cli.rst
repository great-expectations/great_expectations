.. _how_to_guides__creating_and_editing_expectations__how_to_create_a_new_expectation_suite_without_the_cli:

How to create a new Expectation Suite without the CLI
*******************************************************

In some environments, you might not be able to use the :ref:`CLI <command_line>` to create a new Expectation Suite. This guide shows how to create a new Expectation Suite and start adding Expectations using Python code, for example in a Jupyter notebook. Note: If you want to get started creating Expectations with the fewest possible dependencies, e.g. without configuring a Data Context, you should check out :ref:`tutorials__explore_expectations_in_a_notebook`.

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
            - Launched a generic notebook (e.g. ``jupyter notebook``, ``jupyter lab``, etc.)
            - :ref:`Configured a Datasource <how_to_guides__configuring_datasources>`
            - Have a data asset (e.g. a database table) you want to use to create Expectations
            - Have your Data Context configured to save Expectations to your filesystem or another :ref:`Expectation Store <how_to_guides__configuring_metadata_stores>` if you are in a hosted environment

        This code is very similar to the boilerplate you see after creating an :ref:`Expectation Suite using the CLI<how_to_guides__creating_and_editing_expectations__how_to_create_a_new_expectation_suite_using_the_cli>`, with the only difference being that the Expectation Suite is **created** not **loaded** from the Data Context:


        .. code-block:: python

            import great_expectations as ge

            context = ge.data_context.DataContext()
            suite = context.create_expectation_suite(
                "my_suite_name", overwrite_existing=True # Configure these parameters for your needs
            )

        This block just creates an empty Expectation Suite object. Next up, you want to create a Batch to start creating Expectations:

        .. code-block:: python

            batch_kwargs = {
                "datasource": "my_datasource",
                "schema": "my_schema",
                "table": "my_table_name",
            }
            batch = context.get_batch(batch_kwargs, suite)


        **Note:** The `batch_kwargs` depend on what type of data asset you want to connect to (a database table or view, Pandas datasource, etc.). See :ref:`how_to_guides__creating_batches` for your configuration. You can then start creating Expectations on your batch using the methods described in the :ref:`expectation_glossary` and eventually save the Suite to JSON:

        .. code-block:: python

            # Start creating Expectations here
            batch.expect_column_values_to_not_be_null('my_column')

            ...

            # And save the final state to JSON
            batch.save_expectation_suite(discard_failed_expectations=False)

        This will create a JSON file with your Expectation Suite in the Store you have configured, which you can then load and use for :ref:`how_to_guides__validation`.

    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
            - Launched a generic notebook (e.g. ``jupyter notebook``, ``jupyter lab``, etc.)
            - :ref:`Configured a Datasource <how_to_guides__configuring_datasources>`
            - Understand the basics of :ref:`batch requests <reference__core_concepts__datasources>`
            - Have a data asset (e.g. a database table) you want to use to create Expectations
            - Have your Data Context configured to save Expectations to your filesystem or another :ref:`Expectation Store <how_to_guides__configuring_metadata_stores>` if you are in a hosted environment

        This code is very similar to the boilerplate you see after creating an :ref:`Expectation Suite using the CLI<how_to_guides__creating_and_editing_expectations__how_to_create_a_new_expectation_suite_using_the_cli>`, with the only difference being that the Expectation Suite is **created** not **loaded** from the Data Context:


        .. code-block:: python

            import great_expectations as ge
            from great_expectations.core.batch import BatchRequest

            context = ge.data_context.DataContext()
            suite = context.create_expectation_suite(
                "my_suite_name", overwrite_existing=True # Configure these parameters for your needs
            )

        This block just creates an empty Expectation Suite object. Next up, you want to create a Validator to start creating Expectations:

        .. code-block:: python

            batch_request = BatchRequest(
                datasource_name="my_datasource",
                data_connector_name="my_data_connector",
                data_asset_name="my_data_asset"
            )
            validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)


        **Note:** The `batch_request` depends on what type of data asset you want to connect to (a database table or view, Pandas dataframe, etc.). See :ref:`Datasources Reference <reference__core_concepts__datasources>` to learn more about specifying batch requests. You can then start creating Expectations based on your batch using the methods described in the :ref:`expectation_glossary` and eventually save the Suite to JSON:

        .. code-block:: python

            # Start creating Expectations here
            validator.expect_column_values_to_not_be_null('my_column')

            ...

            # And save the final state to JSON
            validator.save_expectation_suite(discard_failed_expectations=False)

        This will create a JSON file with your Expectation Suite in the Store you have configured, which you can then load and use for :ref:`how_to_guides__validation`.

.. discourse::
    :topic_identifier: 240
