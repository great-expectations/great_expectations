.. _how_to_instantiate_a_data_context_on_a_databricks_spark_cluster:

How to instantiate a Data Context on Databricks Spark cluster
=========================================================

This guide will help you instantiate a Data Context on an Databricks Spark cluster.

The guide demonstrates the recommended path for instantiating a Data Context without a full configuration directory and without using the Great Expectations :ref:`command line interface (CLI) <command_line>`.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Followed the Getting Started tutorial and have a basic familiarity with the Great Expectations configuration<getting_started>`.

Steps
-----

This how-to-guide assumes that you are using a Databricks Notebook, and using the Databricks File Store (DBFS) as the Metadata Store and DataDocs store. The `DBFS is a file store <https://docs.databricks.com/data/databricks-file-system.html>`_ that is native to Databricks clusters and Notebooks. Files on DBFS can be written and read as if they were on a local filesystem, just by adding the `/FileStore/` prefix to the path. For information on how to configure Databricks for filesystems on Azure and AWS, please see the associated documentation in the Additional Notes section below.

#. **Install Great Expectations on your Databricks Spark cluster.**

   Copy this code snippet into a cell in your Databricks Spark notebook and run it:

   .. code-block:: python

      dbutils.library.installPyPI("great_expectations")


#. **Configure a Data Context in Memory.**

The following snippet shows Python code that instantiates and configures a Data Context in memory. Copy this snippet into a cell in your Databricks Spark notebook.

.. code-block:: python
   :linenos:

   import great_expectations.exceptions as ge_exceptions
   from great_expectations.data_context.types.base import DataContextConfig
   from great_expectations.data_context import BaseDataContext

   project_config = DataContextConfig(
       config_version=2,
       plugins_directory=None,
       config_variables_file_path=None,

       datasources={
           "my_spark_datasource": {
               "data_asset_type": {
                   "class_name": "SparkDFDataset",
                   "module_name": "great_expectations.dataset",
               },
               "class_name": "SparkDFDatasource",
               "module_name": "great_expectations.datasource",
               "batch_kwargs_generators": {},
           }
       },
       stores={
       "expectations_store": {
           "class_name": "ExpectationsStore",
           "store_backend": {
               "class_name": "TupleFilesystemStoreBackend",
               "base_directory": "REPLACE ME",  # TODO: replace with your value
           },
       },
       "validations_store": {
           "class_name": "ValidationsStore",
           "store_backend": {
               "class_name": "TupleFilesystemStoreBackend",
               "base_directory": "REPLACE ME",  # TODO: replace with your value
           },
       },
       "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
    },
    expectations_store_name="expectations_store",
    validations_store_name="validations_store",
    evaluation_parameter_store_name="evaluation_parameter_store",
    data_docs_sites={
       "local_site": {
           "class_name": "SiteBuilder",
           "store_backend": {
               "class_name": "TupleFilesystemStoreBackend",
               "base_directory": "REPLACE ME",  # TODO: replace with your value
           },
           "site_index_builder": {
               "class_name": "DefaultSiteIndexBuilder",
               "show_cta_footer": True,
           },
       }
    },
    validation_operators={
       "action_list_operator": {
           "class_name": "ActionListValidationOperator",
           "action_list": [
               {
                   "name": "store_validation_result",
                   "action": {"class_name": "StoreValidationResultAction"},
               },
               {
                   "name": "store_evaluation_params",
                   "action": {"class_name": "StoreEvaluationParametersAction"},
               },
               {
                   "name": "update_data_docs",
                   "action": {"class_name": "UpdateDataDocsAction"},
               },
           ],
       }
    },
    anonymous_usage_statistics={
     "enabled": True
    }
    )

   context = BaseDataContext(project_config=project_config)



#. **Configure an Expectation store in DBFS**

   Replace the "REPLACE ME" on lines 27 of the code snippet with the path to your Expectation Store on DBFS.

   .. code-block:: python

      path_to_expectation_store =  "/FileStore/expectations/"

#. **Configure a Validation Result store in DBFS.**

   Replace the "REPLACE ME" on lines 34 of the code snippet with the path to your Validation Store on DBFS.

   .. code-block:: python

      path_to_validation_store =  "/FileStore/validations/"


#. **Configure a Data Docs website in DBFS.**

   Replace the "REPLACE ME" on line 47 of the code snippet with the path to your DataDocs Store on DBFS.

   .. code-block:: python

      path_to_datadocs_store =  "/FileStore/docs/"


#. **Test your configuration.**

   Execute the cell with the snippet above.

   Then copy this code snippet into a cell in your Databricks Spark notebook, run it and verify that no error is displayed:

   .. code-block:: python

      context.list_datasources()


Additional notes
----------------

- If you're continuing to work in a Databricks notebook, the following code-snippet could be used to load and run Expectations on a `csv` file that lives in DBFS.


.. code-block:: python
   :linenos:

   from great_expectations.data_context import BaseDataContext

   file_location = "/FileStore/tables/dc_wikia_data.csv"
   file_type = "csv"

   # CSV options
   infer_schema = "false"
   first_row_is_header = "false"
   delimiter = ","

   # The applied options are for CSV files. For other file types, these will be ignored.
   df = spark.read.format(file_type) \
     .option("inferSchema", infer_schema) \
     .option("header", first_row_is_header) \
     .option("sep", delimiter) \
     .load(file_location)

   context = BaseDataContext(project_config=project_config)
   context.create_expectation_suite("my_new_suite")

   my_batch = context.get_batch({
      "dataset": df,
      "datasource": "my_local_datasource",
   }, "my_new_suite")

   my_batch.expect_table_row_count_to_equal(140)


Additional resources
--------------------
- How to create a Data Source in :ref:`Databricks AWS <_how_to_guides__configuring_datasources__how_to_configure_a_databricks_aws_datasource>`

- How to create a Data Source in :ref:`Databricks Azure <_how_to_guides__configuring_datasources__how_to_configure_a_databricks_azure_datasource>`


.. discourse::
    :topic_identifier: 320
