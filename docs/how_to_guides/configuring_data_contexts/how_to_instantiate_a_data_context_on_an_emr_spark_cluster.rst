.. _how_to_instantiate_a_data_context_on_an_emr_spark_cluster:

How to instantiate a Data Context on an EMR Spark cluster
=========================================================

This guide will help you instantiate a Data Context on an EMR Spark cluster.

The default method for creating and configuring DataContexts is the Great Expectations :ref:`command line interface (CLI) <command_line>`.  It creates the directory structure of the project on your filesystem and its configuration file.


A filesystem to keep the configuration file and a shell for running the CLI that creates it are not readily available
in the EMR Spark environment.


To accommodate for this difference, this guide will show how to configure and instantiate a Data Context
directly in an EMR Spark notebook.


.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - No prerequisites

Steps
-----

The snippet below shows a YAML configuration of a Data Context and Python code that uses this configuration to instantiate
a Data Context. Copy this snippet into a cell in your EMR Spark notebook.

Follow the steps below to update the configuration with values that are specific for your environment.

.. code-block:: python
   :linenos:

   from ruamel.yaml import YAML, YAMLError
   from ruamel.yaml.constructor import DuplicateKeyError
   from ruamel.yaml.comments import CommentedMap

   import great_expectations.exceptions as ge_exceptions
   from great_expectations.data_context.types.base import DataContextConfig
   from great_expectations.data_context import BaseDataContext
   yaml = YAML()


   ge_config_str = """
   # Welcome to Great Expectations! Always know what to expect from your data.
   #
   # Here you can define datasources, batch kwargs generators, integrations and
   # more. This file is intended to be committed to your repo. For help with
   # configuration please:
   #   - Read our docs: https://docs.greatexpectations.io/en/latest/how_to_guides/spare_parts/data_context_reference.html#configuration
   #   - Join our slack channel: http://greatexpectations.io/slack

   # config_version refers to the syntactic version of this config file, and is used in maintaining backwards compatibility
   # It is auto-generated and usually does not need to be changed.
   config_version: 2.0

   # Datasources tell Great Expectations where your data lives and how to get it.
   # You can use the CLI command `great_expectations datasource new` to help you
   # add a new datasource. Read more at https://docs.greatexpectations.io/en/latest/reference/core_concepts/datasource_reference.html
   datasources:
     crimes-in-boston__dir:
       data_asset_type:
         class_name: SparkDFDataset
         module_name: great_expectations.dataset
       class_name: SparkDFDatasource
       module_name: great_expectations.datasource
       batch_kwargs_generators:
   config_variables_file_path: # leave this empty

   # The plugins_directory will be added to your python path for custom modules
   # used to override and extend Great Expectations.
   plugins_directory: # leave this empty

   # Validation Operators are customizable workflows that bundle the validation of
   # one or more expectation suites and subsequent actions. The example below
   # stores validations and send a slack notification. To read more about
   # customizing and extending these, read: https://docs.greatexpectations.io/en/latest/reference/core_concepts/validation_operators_and_actions.html
   validation_operators:
     action_list_operator:
       # To learn how to configure sending Slack notifications during evaluation
       # (and other customizations), read: https://docs.greatexpectations.io/en/latest/autoapi/great_expectations/validation_operators/index.html#great_expectations.validation_operators.ActionListValidationOperator
       class_name: ActionListValidationOperator
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
         # - name: send_slack_notification_on_validation_result
         #   action:
         #     class_name: SlackNotificationAction
         #     # put the actual webhook URL in the uncommitted/config_variables.yml file
         #     slack_webhook: ${validation_notification_slack_webhook}
         #     notify_on: all # possible values: "all", "failure", "success"
         #     renderer:
         #       module_name: great_expectations.render.renderer.slack_renderer
         #       class_name: SlackRenderer

   stores:
   # Stores are configurable places to store things like Expectations, Validations
   # Data Docs, and more. These are for advanced users only - most users can simply
   # leave this section alone.
   #
   # Three stores are required: expectations, validations, and
   # evaluation_parameters, and must exist with a valid store entry. Additional
   # stores can be configured for uses such as data_docs, validation_operators, etc.

     expectations_S3_store:
       class_name: ExpectationsStore
       store_backend:
         class_name: TupleS3StoreBackend
         bucket: TODO: paste the bucket name here
         prefix: TODO: paste the prefix here

     validations_S3_store:
       class_name: ValidationsStore
       store_backend:
         class_name: TupleS3StoreBackend
         bucket: TODO: paste the bucket name here
         prefix: TODO: paste the prefix here

     evaluation_parameter_store:
       # Evaluation Parameters enable dynamic expectations. Read more here:
       # https://docs.greatexpectations.io/en/latest/reference/core_concepts/evaluation_parameters.html
       class_name: EvaluationParameterStore

   expectations_store_name: expectations_S3_store
   validations_store_name: validations_S3_store
   evaluation_parameter_store_name: evaluation_parameter_store

   data_docs_sites:
     # Data Docs make it simple to visualize data quality in your project. These
     # include Expectations, Validations & Profiles. The are built for all
     # Datasources from JSON artifacts in the local repo including validations &
     # profiles from the uncommitted directory. Read more at https://docs.greatexpectations.io/en/latest/reference/core_concepts/data_docs.html
     s3_site:  # this is a user-selected name - you may select your own
       class_name: SiteBuilder
       store_backend:
         class_name: TupleS3StoreBackend
         bucket: TODO: paste the bucket name here
         prefix: TODO: paste the prefix here (optional)
       site_index_builder:
         class_name: DefaultSiteIndexBuilder
         show_cta_footer: true
   anonymous_usage_statistics:
     enabled: true

   """


   try:
       config_dict = yaml.load(ge_config_str)

   except YAMLError as err:
       raise ge_exceptions.InvalidConfigurationYamlError(
           "Your configuration file is not a valid yml file likely due to a yml syntax error:\n\n{}".format(
               err
           )
       )
   except DuplicateKeyError:
       raise ge_exceptions.InvalidConfigurationYamlError(
           "Error: duplicate key found in project YAML file."
       )

   project_config = DataContextConfig.from_commented_map(config_dict)


   context = BaseDataContext(project_config=project_config)


#. **Install Great Expectations on your EMR Spark cluster.**

   Copy this code snippet into a cell in your EMR Spark notebook and run it:

   .. code-block:: python

      sc.install_pypi_package("great_expectations")

#. **Configure an Expectation store in Amazon S3.**

   Replace the "TODO" on line 83 of the code snippet. Follow this :ref:`how-to guide<how_to_guides__configuring_metadata_stores__how_to_configure_an_expectation_store_in_amazon_s3>`.

#. **Configure an Validation Result store in Amazon S3.**

   Replace the "TODO" on line 90 of the code snippet. Follow this :ref:`how-to guide<how_to_guides__configuring_metadata_stores__how_to_configure_a_validation_result_store_in_s3>`.

#. **Configure an Data Docs website in Amazon S3.**

   Replace the "TODO" on line 111 of the code snippet. Follow this :ref:`how-to guide<how_to_guides__configuring_data_docs__how_to_host_and_share_data_docs_on_s3>`.

#. **Test your configuration.**

   Execute the cell with the snippet above.

   Then copy this code snippet into a cell in your EMR Spark notebook, run it and verify that no error is displayed:

   .. code-block:: python

      context.list_datasources()


Additional notes
----------------



Additional resources
--------------------

.. discourse::
    :topic_identifier: 217
