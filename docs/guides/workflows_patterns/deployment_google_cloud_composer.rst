.. _deployment_google_cloud_composer:

Deploying Great Expectations with Google Cloud Composer (Hosted Airflow)
========================================================================

This guide will help you deploy Great Expectations within an Airflow pipeline running on Google Cloud Composer.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`

Steps
-----

Note: These steps are basically following the :ref:`Deploying Great Expectations with Airflow <workflows_patterns__deployment_airflow>` documentation with some items specific to Google Cloud Composer.


1. Set up your Composer environment

    Create a Composer environment using the `instructions located in the Composer documentation <https://cloud.google.com/composer/docs/how-to/managing/creating>`_. Currently Airflow >=1.10.6 is supported by Great Expectations >=0.12.1.

2. Create Expectations

    Create :ref:`Expectations <reference__core_concepts__expectations>` using our guide to :ref:`Creating and Editing Expectations <how_to_guides__creating_and_editing_expectations>`.

    You can store your Expectations anywhere that is accessible to the cloud Composer environment. One simple pattern is to use a folder in the bucket provided by the Composer environment. You can manually push updated expectation JSON files from your version controlled repository via ``gsutil`` or the GCS UI, or automate using Google Cloud Build or any other automation tool.

    .. code-block:: bash

        # copy expectation suites to bucket
        # where COMPOSER_GCS_BUCKET is an environment variable with the name of your bucket
        gsutil cp -r expectations/ gs://${COMPOSER_GCS_BUCKET}/great_expectations/

3. Create your Data Context

    Since we'd prefer not to use the airflow container filesystem to host a :ref:`Data Context <reference__core_concepts__data-context>` as a .yml file, another approach is to instantiate it in a python file either as part of your DAG or imported by your DAG at runtime. :ref:`Follow this guide on How to instantiate a Data Context without a yml file <how_to_guides__configuring_data_contexts__how_to_instantiate_a_data_context_without_a_yml_file>` and see the example below.

    Note: You may want to reference our :ref:`Configuring metadata stores <how_to_guides__configuring_metadata_stores>` and :ref:`Configuring Data Docs <how_to_guides__configuring_data_docs>` how-to guides. All of the stores in the below example are configured to use GCS, however you can use whichever store is applicable to your infrastructure.

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


4. Create a DAG with Validations

    Here we will follow the instructions for :ref:`Running a Validation using a PythonOperator <workflows_patterns__deployment_airflow__running-a-validation-using-a-pythonoperator>`. Note that we will use the Data Context instantiated in the previous step in place of reading from the filesystem as in the linked example.

5. Upload your Expectations and DAG

    Upload your Expectations to your expectation store (as configured in your Data Context). If your expectation store is in your GCS bucket you can use ``gsutil`` to upload the JSON files - just make sure to keep the same directory structure. Alternatively you can automate using something like Google Cloud Build or Github Actions or your favorite CI tool.

    Upload your DAG files to the cloud bucket ``dags/`` folder assigned to your Composer environment.

6. Monitor your deployment

    You can now monitor your deployment just like any other Airflow environment either via the Airflow UI (linked from your cloud platform environments page) or by submitting commands using `Google Cloud Shell <https://cloud.google.com/shell>`_.

    You can raise an ``AirflowException`` if your Validation fails (as in the example here: :ref:`Running a Validation using a PythonOperator <workflows_patterns__deployment_airflow__running-a-validation-using-a-pythonoperator>`) which will show in logs and the UI as in the image below:

.. image:: dag_airflow_example.png
    :width: 800
    :alt: Airflow pipeline with Validations passing and failing.

Additional resources
--------------------

- `Cloud Composer Overview <https://cloud.google.com/composer/docs/concepts/overview>`_

Comments
--------

.. discourse::
   :topic_identifier: 379
