.. _how_to_guides__creating_and_editing_expectations__how_to_create_expectations_that_span_multiple_tables_using_evaluation_parameters:

How to create Expectations that span multiple Batches using Evaluation Parameters
=================================================================================

This guide will help you create Expectations that span multiple :ref:`Batches <reference__core_concepts__batches>` of data using :ref:`Evaluation Parameters <reference__core_concepts__data_context__evaluation_parameter_stores>`. This pattern is useful for things like verifying that row counts between tables stay consistent.

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

          - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
          - Configured a :ref:`Datasource <reference__core_concepts__datasources>` (or Datasources) with at least two Data Assets.
          - Also created :ref:`Expectation Suites <reference__core_concepts__expectations__expectation_suites>` for those Data Assets.
          - Have a working :ref:`Evaluation Parameter Store <reference__core_concepts__data_context__evaluation_parameter_stores>`. (The default in-memory store from ``great_expectations init`` can work for this.)
          - Have a working :ref:`Validation Operator <reference__core_concepts__validation__validation_operator>`. (The default Validation Operator from ``great_expectations init`` can work for this.)

        Steps
        -----

        In a notebook,

        #. **Import great_expectations and instantiate your Data Context**

            .. code-block:: python

                import great_expectations as ge
                context = ge.DataContext()

        #. **Instantiate two Batches**

            We'll call one of these Batches the *upstream* Batch and the other the *downstream* Batch. Evaluation Parameters will allow us to use Validation Results from the upstream Batch as parameters passed into Expectations on the downstream.

            It's common (but not required) for both Batches to come from the same :ref:`Datasource <reference__core_concepts__datasources>` and :ref:`BatchKwargsGenerator <reference__core_concepts__batch_kwargs_generators>`.

            .. code-block:: python

                batch_kwargs_1 = context.build_batch_kwargs("my_datasource", "my_generator_name", "my_data_asset_name_1"),
                upstream_batch = context.get_batch(
                    batch_kwargs_1,
                    expectation_suite_name='my_expectation_suite_1'
                )

                batch_kwargs_2 = context.build_batch_kwargs("my_datasource", "my_generator_name", "my_data_asset_name_2"),
                downstream_batch = context.get_batch(
                    batch_kwargs_2,
                    expectation_suite_name='my_expectation_suite_2'
                )

        #. **Disable interactive evaluation for the downstream Batch.**

            .. code-block:: python

                downstream_batch.set_config_value("interactive_evaluation", False)

            Disabling interactive evaluation allows you to declare an Expectation even when it cannot be evaluated immediately.

        #. **Define an Expectation using an Evaluation Parameter on the downstream Batch.**

            .. code-block:: python

                eval_param_urn = 'urn:great_expectations:validations:my_expectation_suite_1:expect_table_row_count_to_be_between.result.observed_value'
                downstream_batch.expect_table_row_count_to_equal(
                    value={
                        '$PARAMETER': eval_param_urn, # this is the actual parameter we're going to use in the validation
                    }
                )

            The core of this is a ``$PARAMETER : URN`` pair. When Great Expectations encounters a ``$PARAMETER`` flag during validation, it will replace the ``URN`` with a value retrieved from an :ref:`Evaluation Parameter Store <reference__core_concepts__data_context__evaluation_parameter_stores>` or :ref:`Metrics Store <reference__core_concepts__data_context__metrics>`.

            This declaration above includes two ``$PARAMETERS``. The first is the real parameter that will be used after the Expectation Suite is stored and deployed in a Validation Operator. The second parameter supports immediate evaluation in the notebook.

            When executed in the notebook, this Expectation will generate an :ref:`Expectation Validation Result <validation>`. Most values will be missing, since interactive evaluation was disabled.

            .. code-block:: python

                {
                    "meta": {},
                    "success": null,
                    "result": {},
                    "exception_info": null
                }

            .. warning::

                Your URN must be exactly correct in order to work in production. Unfortunately, successful execution at this stage does not guarantee that the URN is specified correctly and that the intended parameters will be available when executed later.

        #. **Save your Expectation Suite**

            .. code-block:: python

                downstream_batch.save_expectation_suite(discard_failed_expectations=False)

            This step is necessary because your ``$PARAMETER`` will only function properly when invoked within a Validation operation with multiple Batches. The simplest way to execute such an operation is through a :ref:`Validation Operator <reference__core_concepts__validation__validation_operator>`, and Validation Operators are configured to load Expectation Suites from Expectation Stores, not memory.

        #. **Execute an existing Validation Operator on your upstream and downstream batches.**

            You can do this within your notebook by running ``context.run_validation_operator``. You can use the same ``batch_kwargs`` from the top of your notebook---they'll be used to fetch the same data.

            .. code-block:: python

                results = context.run_validation_operator(
                    "action_list_operator",
                    assets_to_validate=[
                        (batch_kwargs_1, "my_expectation_suite_1"),
                        (batch_kwargs_2, "my_expectation_suite_2"),
                    ]
                )

        #. **Rebuild Data Docs and review results in docs.**

            You can do this within your notebook by running:

            .. code-block:: python

                context.build_data_docs()

            You can also execute from the command line with:

            .. code-block:: bash

                great_expectations docs build

            Once your Docs rebuild, open them in a browser and navigate to the page for the new Validation Result.

            If your Evaluation Parameter was executed successfully, you'll see something like this:

            .. image:: /images/evaluation_parameter_success.png

            |

            If it encountered an error, you'll see something like this. The most common problem is a mis-specified URN name.

            .. image:: /images/evaluation_parameter_error.png

            .. warning::

                In general, the development loop for testing and debugging URN and Evaluation Parameters is not very user-friendly. We plan to simplify this workflow in the future. In the meantime, we welcome questions in the `Great Expectations discussion forum <https://discuss.great_expectations.io>`_ and `Slack channel <https://great_expectations.io/slack>`_.

    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

          - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
          - Configured a :ref:`Datasource <reference__core_concepts__datasources>` (or Datasources) with at least two Data Assets and understand the basics of batch requests
          - Also created :ref:`Expectation Suites <how_to_guides__creating_and_editing_expectations>` for those Data Assets.
          - Have a working :ref:`Evaluation Parameter Store <reference__core_concepts__data_context__evaluation_parameter_stores>`. (The default in-memory store from ``great_expectations init`` can work for this.)
          - Have a working :ref:`Validation Operator <reference__core_concepts__validation__validation_operator>`. (The default Validation Operator from ``great_expectations init`` can work for this.)

        Steps
        -----

        In a notebook,

        #. **Import great_expectations and instantiate your Data Context**

            .. code-block:: python

                import great_expectations as ge
                context = ge.DataContext()

        #. **Instantiate two Validators, one for each Data Asset**

            We'll call one of these Validators the *upstream* Validator and the other the *downstream* Validator. Evaluation Parameters will allow us to use Validation Results from the upstream Validator as parameters passed into Expectations on the downstream.

            It's common (but not required) for both Batch Requests to have the same :ref:`Datasource and Data Connector <reference__core_concepts__datasources>`.

            .. code-block:: python

                batch_request_1 = BatchRequest(
                    datasource_name="my_datasource",
                    data_connector_name="my_data_connector",
                    data_asset_name="my_data_asset_1"
                )
                upstream_validator = context.get_validator(batch_request=batch_request_1, expectation_suite_name="my_expectation_suite_1")

                batch_request_2 = BatchRequest(
                    datasource_name="my_datasource",
                    data_connector_name="my_data_connector",
                    data_asset_name="my_data_asset_2"
                )
                downstream_validator = context.get_validator(batch_request=batch_request_2, expectation_suite_name="my_expectation_suite_2")

        #. **Disable interactive evaluation for the downstream Validator.**

            .. code-block:: python

                downstream_validator.interactive_evaluation = False

            Disabling interactive evaluation allows you to declare an Expectation even when it cannot be evaluated immediately.

        #. **Define an Expectation using an Evaluation Parameter on the downstream Validator.**

            .. code-block:: python

                eval_param_urn = 'urn:great_expectations:validations:my_expectation_suite_1:expect_table_row_count_to_be_between.result.observed_value'
                downstream_validator.expect_table_row_count_to_equal(
                    value={
                        '$PARAMETER': eval_param_urn, # this is the actual parameter we're going to use in the validation
                    }
                )

            The core of this is a ``$PARAMETER : URN`` pair. When Great Expectations encounters a ``$PARAMETER`` flag during validation, it will replace the ``URN`` with a value retrieved from an :ref:`Evaluation Parameter Store <reference__core_concepts__data_context__evaluation_parameter_stores>` or :ref:`Metrics Store <reference__core_concepts__data_context__metrics>`.

            This declaration above includes two ``$PARAMETERS``. The first is the real parameter that will be used after the Expectation Suite is stored and deployed in a Validation Operator. The second parameter supports immediate evaluation in the notebook.

            When executed in the notebook, this Expectation will generate an :ref:`Expectation Validation Result <validation>`. Most values will be missing, since interactive evaluation was disabled.

            .. code-block:: python

                {
                  "result": {},
                  "success": null,
                  "meta": {},
                  "exception_info": {
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }
                }

            .. warning::

                Your URN must be exactly correct in order to work in production. Unfortunately, successful execution at this stage does not guarantee that the URN is specified correctly and that the intended parameters will be available when executed later.

        #. **Save your Expectation Suite**

            .. code-block:: python

                downstream_validator.save_expectation_suite(discard_failed_expectations=False)

            This step is necessary because your ``$PARAMETER`` will only function properly when invoked within a Validation operation with multiple Validators. The simplest way to execute such an operation is through a :ref:`Validation Operator <reference__core_concepts__validation__validation_operator>`, and Validation Operators are configured to load Expectation Suites from Expectation Stores, not memory.

        #. **Execute an existing Validation Operator on your upstream and downstream Validators.**

            You can do this within your notebook by running ``context.run_validation_operator``.

            .. code-block:: python

                results = context.run_validation_operator(
                    "action_list_operator",
                    assets_to_validate=[
                        upstream_validator,
                        downstream_validator
                    ]
                )

        #. **Rebuild Data Docs and review results in docs.**

            You can do this within your notebook by running:

            .. code-block:: python

                context.build_data_docs()

            You can also execute from the command line with:

            .. code-block:: bash

                great_expectations docs build

            Once your Docs rebuild, open them in a browser and navigate to the page for the new Validation Result.

            If your Evaluation Parameter was executed successfully, you'll see something like this:

            .. image:: /images/evaluation_parameter_success.png

            |

            If it encountered an error, you'll see something like this. The most common problem is a mis-specified URN name.

            .. image:: /images/evaluation_parameter_error.png

            .. warning::

                In general, the development loop for testing and debugging URN and Evaluation Parameters is not very user-friendly. We plan to simplify this workflow in the future. In the meantime, we welcome questions in the `Great Expectations discussion forum <https://discuss.great_expectations.io>`_ and `Slack channel <https://great_expectations.io/slack>`_.

Comments
--------

.. discourse::
    :topic_identifier: 206
