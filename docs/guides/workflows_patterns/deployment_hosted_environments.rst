.. _deployment_hosted_enviroments:

#################################
Deploying Great Expectations in a hosted environment without file system or CLI
#################################

Great Expectations can be used powerfully in a hosted environment like Databricks Notebooks or hosted Jupyter notebooks.

The guide demonstrates the recommended path for instantiating a Data Context without a full configuration directory and without using the Great Expectations :ref:`command line interface (CLI) <command_line>`.


.. admonition:: An interactive version can be found here :
    .. image:: https://mybinder.org/badge_logo.svg
        :target: https://mybinder.org/v2/gh/superconductive/JupyterBinders/master?filepath=PandasDataContext.ipynb


.. code-block:: python

    # packages that are needed to run the example.
    import json
    import os

    import great_expectations as ge
    from great_expectations import DataContext
    from great_expectations.data_context.types.base import DataContextConfig
    from great_expectations.data_context import BaseDataContext
    import great_expectations.exceptions as ge_exceptions


The paths are going to be different in each environment. The following path will run with the linked Jupyter Binder instance.
additional documentation on how data sources and metadata stores can be configured can be found in the following documentation

- :ref:`Databricks AWS Datasource  <how_to_guides__configuring_datasources__how_to_configure_a_databricks_aws_datasource>`
- :ref:`Databricks Azure Datasource  <how_to_guides__configuring_datasources__how_to_configure_a_databricks_azure_datasource>`


**DataContext Configuration**

The follow code will create the configuration for project. The configuration is ``project_config``
datasource is set as ``PandasDatset``, and we will be linking it to our example dataset.
- our ``expectation_store`` is set to ``/home/jovyan/testing/expectations``, which is the folder in our Docker container
- our ``validations_store`` is set to ``/home/jovyan/testing/validations``, which is the folder in our Docker container
- our ``local_site`` is set to ``/home/jovyan/testing/docs``, which is the folder in our Docker container


.. code-block:: python

    project_config = DataContextConfig(
        config_version=2,
        plugins_directory=None,
        config_variables_file_path=None,
        datasources={
           "my_local_datasource": {
               "data_asset_type": {
                   "class_name": "PandasDataset",
                   "module_name": "great_expectations.dataset",
               },
               "class_name": "PandasDatasource",
               "module_name": "great_expectations.datasource",
               "batch_kwargs_generators": {},
           }
        },

        stores={
           "expectations_store": {
               "class_name": "ExpectationsStore",
               "store_backend": {
                   "class_name": "TupleFilesystemStoreBackend",
                   "base_directory": "/home/jovyan/testing/expectations/"
               },
           },
           "validations_store": {
               "class_name": "ValidationsStore",
               "store_backend": {
                   "class_name": "TupleFilesystemStoreBackend",
                   "base_directory": "/home/jovyan/testing/validations/"

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
                   "base_directory": "/home/jovyan/testing/data_docs/",

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

**Creating ** ``BaseDataContext``
- Create a Great Expectations ``data_context`` using the config you created above.

.. code-block:: python

    context = BaseDataContext(project_config=project_config)


**Creating Expectation Suite**
- Use the ``DataContext`` to create a new Expectation Suite.
- **Note** The Expectation Suite will be saved in the ``/home/jovyan/testing/expectations/`` directory we defined above

.. code-block:: python

    context.create_expectation_suite("my_new_suite")

**Creating Batch**
1. ``path`` to the dataset, which is ``dc-wikia-data.csv``
2. ``datasource`` that we have defined in the config
3. Name of Expectation Suite that we created ``my_new_suite``


.. code-block:: python

    my_batch = context.get_batch({
          "path": "dc-wikia-data.csv",
          "datasource": "my_local_datasource",
       }, "my_new_suite")


**Running our First Expectation**

.. code-block:: python

    my_batch.expect_table_columns_to_match_ordered_list(["page_id",
          "name",
          "urlslug",
          "ID",
          "ALIGN",
          "EYE",
          "HAIR",
          "SEX",
          "GSM",
          "ALIVE",
          "APPEARANCES",
          "FIRST APPEARANCE",
          "YEAR"])


- Output should look like the following


.. code-block:: bash

    {
      "success": true,
      "meta": {},
      "exception_info": null,
      "result": {
        "observed_value": [
          "page_id",
          "name",
          "urlslug",
          "ID",
          "ALIGN",
          "EYE",
          "HAIR",
          "SEX",
          "GSM",
          "ALIVE",
          "APPEARANCES",
          "FIRST APPEARANCE",
          "YEAR"
        ]
      }
    }
