.. _deployment_google_cloud_composer:

Deploying Great Expectations with Google Cloud Composer (Hosted Airflow)
========================================================================

This guide will help you deploy Great Expectations within an Airflow pipeline running on Google Cloud Composer.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

# TODO: fill in prerequisites
  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`

Steps
-----

Note: These steps are basically following the `Deploying Great Expectations with Airflow <https://docs.greatexpectations.io/en/latest/guides/workflows_patterns/deployment_airflow.html>`_ documentation with some items specific to Google Cloud Composer.


#. Set up your composer environment

Create a composer environment using the `instructions located in the composer documentation <https://cloud.google.com/composer/docs/how-to/managing/creating>`_. Currently Airflow >=1.10.6 is supported by Great Expectations >=0.12.1.

#. Create expectations

Create expectations using https://docs.greatexpectations.io/en/latest/guides/how_to_guides/creating_and_editing_expectations.html
You can store your expectations anywhere that is accessible to the cloud composer environment. One simple pattern is to use a folder in the bucket provided by the composer environment. You can manually push updated expectation json via `gsutil` or the GCS UI, or automate using Google Cloud Build or any other automation tool.

#. Create your Data Context

Since we'd prefer not to use the airflow container filesystem to host a Data Context as a .yml file, another approach is to instantiate it in a python file either as part of your dag or imported by your dag at runtime. `Follow this guide on How to instantiate a Data Context without a yml file <https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file.html>`_. Instead of copying the snippet into your EMR Spark notebook, you can simply create it in your DAG or in a separate file and import into your DAG file.

If you used your composer bucket to store all data, our expectation store will be within that bucket.

.. code-block:: python

    project_config = DataContextConfig(
        config_version=2,
        plugins_directory=None,
        config_variables_file_path=None,
        datasources={
            "my_pandas_datasource": {
                "data_asset_type": {
                    "class_name": "PandasDataset",
                    "module_name": "great_expectations.dataset",
                },
                "class_name": "PandasDatasource",
                "module_name": "great_expectations.datasource",
                "batch_kwargs_generators": {
                    # TODO: Enter these here or later in your validations
                },
            }
        },
        stores={
            "expectations_GCS_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleGCSStoreBackend",
                    "project": "REPLACE ME",  # TODO: replace with your value
                    "bucket": "REPLACE ME",  # TODO: replace with your value
                    "prefix": "REPLACE ME",  # TODO: replace with your value
                },
            },
            "validations_GCS_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleGCSStoreBackend",
                    "project": "REPLACE ME",  # TODO: replace with your value
                    "bucket": "REPLACE ME",  # TODO: replace with your value
                    "prefix": "REPLACE ME",  # TODO: replace with your value
                },
            },
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
        },
        expectations_store_name="expectations_GCS_store",
        validations_store_name="validations_GCS_store",
        evaluation_parameter_store_name="evaluation_parameter_store",
        data_docs_sites={
            "gs_site": {
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleGCSStoreBackend",
                    "project": "REPLACE ME",  # TODO: replace with your value
                    "bucket": "REPLACE ME",  # TODO: replace with your value
                    "prefix": "REPLACE ME",  # TODO: replace with your value
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                    "show_cta_footer": True,
                },
                "show_how_to_buttons": True,
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


#. Create a DAG with validations

Here we will follow the instructions for `Running a Validation using a PythonOperator<https://docs.greatexpectations.io/en/latest/guides/workflows_patterns/deployment_airflow.html#running-a-validation-using-a-pythonoperator>`_ using the data context instantiated in the previous step in place of reading from the filesystem as in the linked example.

#. Upload your expectations and dag

Upload your expectations to your expectation store (as configured in your data context). If your expectation store is in your GCS bucket you can use `gsutil` to upload the json files - just make sure to keep the same directory structure. Alternatively you can automate using something like Google Cloud Build or Github Actions or your favorite CI tool.
Upload your dag files to the cloud bucket `dags/` folder assigned to your composer environment.

#. Monitor your deployment

You can now monitor your deployment just like any other Airflow environment either via the Airflow UI (linked from your cloud platform environments page) or by submitting commands using Google Cloud Shell.


Additional resources
--------------------

- `Cloud Composer Overview <https://cloud.google.com/composer/docs/concepts/overview>`_

Comments
--------

.. discourse::
   :topic_identifier: 379
