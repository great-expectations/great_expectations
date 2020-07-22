.. _how_to_instantiate_a_data_context_on_an_emr_spark_cluster:

How to instantiate a Data Context on an EMR Spark cluster
=========================================================

This guide will help you instantiate a Data Context on an EMR Spark cluster.


The guide demonstrates the recommended path for instantiating a Data Context without a full configuration directory and without using the Great Expectations :ref:`command line interface (CLI) <command_line>`.


.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Followed the Getting Started tutorial and have a basic familiarity with the Great Expectations configuration<getting_started>`.

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
   config_version: 2.0

   datasources:
     crimes-in-boston__dir:
       data_asset_type:
         class_name: SparkDFDataset
         module_name: great_expectations.dataset
       class_name: SparkDFDatasource
       module_name: great_expectations.datasource
       batch_kwargs_generators:
   config_variables_file_path: # leave this empty

   plugins_directory: # leave this empty

   validation_operators:
     action_list_operator:
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

   stores:
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
       class_name: EvaluationParameterStore

   expectations_store_name: expectations_S3_store
   validations_store_name: validations_S3_store
   evaluation_parameter_store_name: evaluation_parameter_store

   data_docs_sites:
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
