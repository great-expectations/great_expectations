.. _how_to_instantiate_a_data_context_on_a_databricks_spark_cluster:

How to instantiate a Data Context on Databricks Spark cluster
=========================================================

This guide will help you instantiate a Data Context on an Databricks Spark cluster.

The guide demonstrates the recommended path for instantiating a Data Context without a full configuration directory and without using the Great Expectations :ref:`command line interface (CLI) <command_line>`.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Followed the Getting Started tutorial and have a basic familiarity with the Great Expectations configuration<getting_started>`.

Steps
-----

This how-to-guide assumes that you are using a Databricks Notebook, and using the Databricks File Store (DBFS) as the Metadata Store and DataDocs store. The `DBFS is a file store <https://docs.databricks.com/data/databricks-file-system.html>`_ that is native to Databricks clusters and Notebooks. Files on DBFS can be written and read as if they were on a local filesystem, just by `adding the /dbfs/ prefix to the path <https://docs.databricks.com/data/databricks-file-system.html#local-file-apis>`_. For information on how to configure Databricks for filesystems on Azure and AWS, please see the associated documentation in the Additional Notes section below.

1. **Install Great Expectations on your Databricks Spark cluster.**

   Copy this code snippet into a cell in your Databricks Spark notebook and run it:

   .. code-block:: python

      dbutils.library.installPyPI("great_expectations")


2. **Configure a Data Context in code.**

Follow the steps for creating an in-code Data Context in :ref:`How to instantiate a Data Context without a yml file <how_to_guides__configuring_data_contexts__how_to_instantiate_a_data_context_without_a_yml_file>` using the FilesystemStoreBackendDefaults or configuring stores as in the code block below.

.. note::
   If you are using DBFS for your stores, make sure to set the ``root_directory`` of FilesystemStoreBackendDefaults to ``/dbfs/`` or ``/dbfs/FileStore/`` to make sure you are writing to DBFS and not the Spark driver node filesystem. If you have mounted another file store (e.g. s3 bucket) to use instead of DBFS, you can use that path here instead.

.. code-block:: python
   :linenos:

   from great_expectations.data_context.types.base import DataContextConfig
   from great_expectations.data_context import BaseDataContext

   data_context_config = DataContextConfig(
      datasources={<set_your_datasources_here>},
      store_backend_defaults=FilesystemStoreBackendDefaults(root_directory="/dbfs/FileStore/"),
   )
   context = BaseDataContext(project_config=data_context_config)

You can have more fine-grained control over where your stores are located by passing the ``stores`` parameter to DataContextConfig as in the following example.

.. code-block:: python
   :linenos:

   data_context_config = DataContextConfig(
      datasources={<set_your_datasources_here>},
      stores={
          "expectations_store": {
              "class_name": "ExpectationsStore",
              "store_backend": {
                  "class_name": "TupleFilesystemStoreBackend",
                  "base_directory": "/dbfs/FileStore/path_to_your_expectations_store/",
              },
          },
          "validations_store": {
              "class_name": "ValidationsStore",
              "store_backend": {
                  "class_name": "TupleFilesystemStoreBackend",
                  "base_directory": "/dbfs/FileStore/path_to_your_validations_store/",
              },
          },
          "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
       },
      store_backend_defaults=FilesystemStoreBackendDefaults(),
   )

3. **Test your configuration.**

   After you have created your Data Context, copy this code snippet into a cell in your Databricks Spark notebook, run it and verify that no error is displayed:

   .. code-block:: python

      context.list_datasources()


Additional notes
----------------

- If you're continuing to work in a Databricks notebook, the following code-snippet could be used to load and run Expectations on a `csv` file that lives in DBFS.


.. code-block:: python

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
- How to create a Data Source in :ref:`Databricks AWS <how_to_guides__configuring_datasources__how_to_configure_a_databricks_aws_datasource>`
- How to create a Data Source in :ref:`Databricks Azure <how_to_guides__configuring_datasources__how_to_configure_a_databricks_azure_datasource>`

.. discourse::
    :topic_identifier: 320
