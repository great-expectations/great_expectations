.. _how_to_guides__creating_and_editing_expectations__how_to_create_a_new_expectation_suite_without_a_sample_batch:

How to create a new Expectation Suite without a sample Batch
************************************************************

This guide shows how to create an Expectation Suite without a sample Batch.

Here are some reasons why you may wish to do this but please contact us if there are other use cases we have not yet considered:

#. You don't have a sample.

#. You don't currently have access to the data to make a sample.

#. You know exactly how you want your Expectations to be configured.

#. You want to create Expectations parametrically (you can also do this in interactive mode).

#. You don't want to spend the time to validate against a sample.

-----
Steps
-----

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
            - Launched a generic notebook (e.g. ``jupyter notebook``, ``jupyter lab``, etc.)
            - Have your Data Context configured to save Expectations to your filesystem or another :ref:`Expectation Store <how_to_guides__configuring_metadata_stores>` if you are in a hosted environment

        #. **Load your Data Context**

            This code is very similar to the boilerplate you see after creating an :ref:`Expectation Suite using the CLI<how_to_guides__creating_and_editing_expectations__how_to_create_a_new_expectation_suite_using_the_cli>`, with the only difference being that the Expectation Suite is **created** not **loaded** from the Data Context:


            .. code-block:: python

                import great_expectations as ge

                context = ge.data_context.DataContext()
                suite = context.create_expectation_suite(
                    "my_suite_name", overwrite_existing=True # Configure these parameters for your needs
                )

            This block just creates an empty Expectation Suite object. Next up, start creating your Expectations.

        #. **Create Expectation Configurations in your notebook**

            You are adding Expectation configurations to the suite. Since there is no sample Batch of data, no validation happens during this process. To illustrate how to do this, consider a hypothetical example. Suppose that you have a table with the columns ``account_id``, ``user_id``, ``transaction_id``, ``transaction_type``, and ``transaction_amt_usd``. Then the following code snipped adds an expectation that the columns of the actual table will appear in the order specified above:

            .. code-block:: python

                # Create an Expectation
                expectation_configuration = ExpectationConfiguration(
                    # Name of expectation type being added
                    expectation_type="expect_table_columns_to_match_ordered_list",
                    # These are the arguments of the expectation
                    # The keys allowed in the dictionary are Parameters and
                    # Keyword Arguments of this Expectation Type
                    kwargs={
                        "column_list": [
                            "account_id", "user_id", "transaction_id", "transaction_type", "transaction_amt_usd"
                        ]
                    },
                    # This is how you can optionally add a comment about this expectation.
                    # It will be rendered in Data Docs.
                    # See this guide for details:
                    # `How to add comments to Expectations and display them in Data Docs`.
                    meta={
                        "notes": {
                            "format": "markdown",
                            "content": "Some clever comment about this expectation. **Markdown** `Supported`"
                        }
                    }
                )
                # Add the Expectation to the suite
                suite.add_expectation(expectation_configuration=expectation_configuration)


            Here are a few more example expectations for this dataset:

            .. code-block:: python

                expectation_configuration = ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_in_set",
                    kwargs={
                        "column": "transaction_type",
                        "value_set": ["purchase", "refund", "upgrade"]
                    },
                    # Note optional comments omitted
                )
                suite.add_expectation(expectation_configuration=expectation_configuration)



                expectation_configuration = ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={
                        "column": "account_id",
                        "mostly": 1.0,
                    },
                    meta={
                        "notes": {
                            "format": "markdown",
                            "content": "Some clever comment about this expectation. **Markdown** `Supported`"
                        }
                    }
                )
                suite.add_expectation(expectation_configuration=expectation_configuration)



                expectation_configuration = ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={
                        "column": "user_id",
                        "mostly": 0.75,
                    },
                    meta={
                        "notes": {
                            "format": "markdown",
                            "content": "Some clever comment about this expectation. **Markdown** `Supported`"
                        }
                    }
                )
                suite.add_expectation(expectation_configuration=expectation_configuration)

            You can see all the available Expectations in the :ref:`expectation_glossary`.

        #. **Save your Expectation Suite**

            Run this in a cell in your notebook:

            .. code-block:: python

                context.save_expectation_suite(suite, expectation_suite_name)

            This will create a JSON file with your Expectation Suite in the Store you have configured, which you can then load and use for :ref:`how_to_guides__validation`.


    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
            - Have your Data Context configured to save Expectations to your filesystem or another :ref:`Expectation Store <how_to_guides__configuring_metadata_stores>` if you are in a hosted environment

        #. **Use the CLI to generate a helper notebook**

            From the command line, run:

            .. code-block:: bash

                great_expectations --v3-api suite new

        #. **Create Expectation Configurations in the helper notebook**

            You are adding Expectation configurations to the suite. Since there is no sample Batch of data, no validation happens during this process. To illustrate how to do this, consider a hypothetical example. Suppose that you have a table with the columns ``account_id``, ``user_id``, ``transaction_id``, ``transaction_type``, and ``transaction_amt_usd``. Then the following code snipped adds an expectation that the columns of the actual table will appear in the order specified above:

            .. code-block:: python

                # Create an Expectation
                expectation_configuration = ExpectationConfiguration(
                    # Name of expectation type being added
                    expectation_type="expect_table_columns_to_match_ordered_list",
                    # These are the arguments of the expectation
                    # The keys allowed in the dictionary are Parameters and
                    # Keyword Arguments of this Expectation Type
                    kwargs={
                        "column_list": [
                            "account_id", "user_id", "transaction_id", "transaction_type", "transaction_amt_usd"
                        ]
                    },
                    # This is how you can optionally add a comment about this expectation.
                    # It will be rendered in Data Docs.
                    # See this guide for details:
                    # `How to add comments to Expectations and display them in Data Docs`.
                    meta={
                        "notes": {
                            "format": "markdown",
                            "content": "Some clever comment about this expectation. **Markdown** `Supported`"
                        }
                    }
                )
                # Add the Expectation to the suite
                suite.add_expectation(expectation_configuration=expectation_configuration)


            Here are a few more example expectations for this dataset:

            .. code-block:: python

                expectation_configuration = ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_in_set",
                    kwargs={
                        "column": "transaction_type",
                        "value_set": ["purchase", "refund", "upgrade"]
                    },
                    # Note optional comments omitted
                )
                suite.add_expectation(expectation_configuration=expectation_configuration)



                expectation_configuration = ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={
                        "column": "account_id",
                        "mostly": 1.0,
                    },
                    meta={
                        "notes": {
                            "format": "markdown",
                            "content": "Some clever comment about this expectation. **Markdown** `Supported`"
                        }
                    }
                )
                suite.add_expectation(expectation_configuration=expectation_configuration)



                expectation_configuration = ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={
                        "column": "user_id",
                        "mostly": 0.75,
                    },
                    meta={
                        "notes": {
                            "format": "markdown",
                            "content": "Some clever comment about this expectation. **Markdown** `Supported`"
                        }
                    }
                )
                suite.add_expectation(expectation_configuration=expectation_configuration)

            You can see all the available Expectations in the :ref:`expectation_glossary`.

        #. **Save your Expectation Suite**

            Run the final cell in the helper notebook to save your Expectation Suite.

            This will create a JSON file with your Expectation Suite in the Store you have configured, which you can then load and use for :ref:`how_to_guides__validation`.

.. discourse::
    :topic_identifier: 555