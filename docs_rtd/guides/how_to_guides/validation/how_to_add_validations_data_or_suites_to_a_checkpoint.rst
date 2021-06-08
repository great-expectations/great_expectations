.. _how_to_guides__validation__how_to_add_validations_data_or_suites_to_a_checkpoint:

How to add validations, data, or suites to a Checkpoint
=======================================================

This guide will help you add validations, data or suites to an existing Checkpoint.
This is useful if you want to aggregate individual validations (across suites or datasources) into a single Checkpoint.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
    - You have an :ref:`existing Expectation Suite <how_to_guides__creating_and_editing_expectations>`
    - You have an :ref:`existing Checkpoint <how_to_guides__validation__how_to_create_a_new_checkpoint>`

Steps
-----

.. content-tabs::

    .. tab-container:: tab0
        :title: Docs for Legacy Checkpoints (<=0.13.7)

        1. First, open your existing Checkpoint in a text editor.
        It will look similar to this:

        .. code-block:: yaml

            validation_operator_name: action_list_operator
            batches:
              - batch_kwargs:
                  path: /home/me/my_project/source_files/npi.csv
                  datasource: files_datasource
                  reader_method: read_csv
                expectation_suite_names:
                  - npi.warning

        2. To add a second suite (in this example we add ``npi.critical``) to your Checkpoint modify the file to look like this:

        .. code-block:: yaml

            validation_operator_name: action_list_operator
            batches:
              - batch_kwargs:
                  path: /home/me/my_project/source_files/npi.csv
                  datasource: files_datasource
                  reader_method: read_csv
                expectation_suite_names:
                  - npi.warning
                  - npi.critical

        3. To add a second validation of a batch of data (in this case a table named ``npi`` from a datasource named ``data_lake``) to your Checkpoint modify the file to look like this:

        .. code-block:: yaml

            validation_operator_name: action_list_operator
            batches:
              - batch_kwargs:
                  path: /home/me/my_project/source_files/npi.csv
                  datasource: files_datasource
                  reader_method: read_csv
                expectation_suite_names:
                  - npi.warning
                  - another_suite
              - batch_kwargs:
                  table: npi
                  datasource: data_lake
                expectation_suite_names:
                  - npi.warning

    .. tab-container:: tab1
        :title: Docs for Class-Based Checkpoints (>=0.13.8)

        1. First, open your existing Checkpoint in a text editor.
        It will look similar to this:

        .. code-block:: yaml

            name: my_checkpoint
            config_version: 1
            class_name: Checkpoint
            run_name_template: "%Y-%M-foo-bar-template-$VAR"
            validations:
              - batch_request:
                  datasource_name: my_datasource
                  data_connector_name: my_data_connector
                  data_asset_name: users
                  data_connector_query:
                    index: -1
                expectation_suite_name: users.warning
                action_list:
                    - name: store_validation_result
                      action:
                        class_name: StoreValidationResultAction
                    - name: store_evaluation_params
                      action:
                        class_name: StoreEvaluationParametersAction
                    - name: update_data_docs
                      action:
                        class_name: UpdateDataDocsAction
                evaluation_parameters:
                  param1: "$MY_PARAM"
                  param2: 1 + "$OLD_PARAM"
                runtime_configuration:
                  result_format:
                    result_format: BASIC
                    partial_unexpected_count: 20

        2. To add a second Expectation Suite (in this example we add ``users.error``) to your Checkpoint configuration, modify the file to look like this:

        .. code-block:: yaml

            name: my_checkpoint
            config_version: 1
            class_name: Checkpoint
            run_name_template: "%Y-%M-foo-bar-template-$VAR"
            validations:
              - batch_request:
                  datasource_name: my_datasource
                  data_connector_name: my_data_connector
                  data_asset_name: users
                  data_connector_query:
                    index: -1
                expectation_suite_name: users.warning
                action_list:
                    - name: store_validation_result
                      action:
                        class_name: StoreValidationResultAction
                    - name: store_evaluation_params
                      action:
                        class_name: StoreEvaluationParametersAction
                    - name: update_data_docs
                      action:
                        class_name: UpdateDataDocsAction
                evaluation_parameters:
                  param1: "$MY_PARAM"
                  param2: 1 + "$OLD_PARAM"
                runtime_configuration:
                  result_format:
                    result_format: BASIC
                    partial_unexpected_count: 20
              - batch_request:
                  datasource_name: my_datasource
                  data_connector_name: my_data_connector
                  data_asset_name: users
                  data_connector_query:
                    index: -1
                expectation_suite_name: users.error
                action_list:
                    - name: store_validation_result
                      action:
                        class_name: StoreValidationResultAction
                    - name: store_evaluation_params
                      action:
                        class_name: StoreEvaluationParametersAction
                    - name: update_data_docs
                      action:
                        class_name: UpdateDataDocsAction
                evaluation_parameters:
                  param1: "$MY_PARAM"
                  param2: 1 + "$OLD_PARAM"
                runtime_configuration:
                  result_format:
                    result_format: BASIC
                    partial_unexpected_count: 20

        3. The flexibility of easily adding multiple validations of batches of data with different Expectation Suites and specific actions can be demonstrated using the following example of a Checkpoint configuration file:

        .. code-block:: yaml

            name: my_fancy_checkpoint
            config_version: 1
            class_name: Checkpoint
            run_name_template: "%Y-%M-foo-bar-template-$VAR"
            expectation_suite_name: users.delivery
            action_list:
                - name: store_validation_result
                  action:
                    class_name: StoreValidationResultAction
                - name: store_evaluation_params
                  action:
                    class_name: StoreEvaluationParametersAction
                - name: update_data_docs
                  action:
                    class_name: UpdateDataDocsAction
            validations:
              - batch_request:
                  datasource_name: my_datasource
                  data_connector_name: my_data_connector
                  data_asset_name: users
                  data_connector_query:
                    index: 0
                expectation_suite_name: users.warning
              - batch_request:
                  datasource_name: my_datasource
                  data_connector_name: my_special_data_connector
                  data_asset_name: users
                  data_connector_query:
                    index: -1
                expectation_suite_name: users.error
              - batch_request:
                  datasource_name: my_datasource
                  data_connector_name: my_other_data_connector
                  data_asset_name: users
                  data_connector_query:
                    batch_filter_parameters:
                      name: Titanic
                action_list:
                  - name: quarantine_failed_data
                    action:
                      class_name: CreateQuarantineData
                  - name: advance_passed_data
                    action:
                      class_name: CreateQuarantineData
            evaluation_parameters:
              param1: "$MY_PARAM"
              param2: 1 + "$OLD_PARAM"
            runtime_configuration:
              result_format:
                result_format: BASIC
                partial_unexpected_count: 20

        According to this configuration, the locally-specified Expectation Suite ``users.warning`` is run against the ``batch_request`` that employs ``my_data_connector`` with the results processed by the actions specified in the top-level ``action_list``.
        Similarly, the locally-specified Expectation Suite ``users.error`` is run against the ``batch_request`` that employs ``my_special_data_connector`` with the results also processed by the actions specified in the top-level ``action_list``.
        In addition, the top-level Expectation Suite ``users.delivery`` is run against the ``batch_request`` that employs ``my_other_data_connector`` with the results processed by the union of actions in the locally-specified ``action_list`` and in the top-level ``action_list``.

        Please see :ref:`How to configure a new Checkpoint using "test_yaml_config" <how_to_guides_how_to_configure_a_new_checkpoint_using_test_yaml_config>` for additional Checkpoint configuration examples (including the convenient templating mechanism).


Additional notes
----------------

.. tip::

    This is a good way to aggregate validations in a complex pipeline.
    You could use this feature to **validate multiple source files before and after their ingestion into your data lake**.

If it would be useful to you, please comment with a +1 and feel free to add any suggestions or questions below.

If you want to be a real hero, we'd welcome a pull request. Please see :ref:`the Contributing tutorial <contributing>` and :ref:`how_to_guides__miscellaneous__how_to_write_a_how_to_guide` to get started.

.. discourse::
    :topic_identifier: 216
