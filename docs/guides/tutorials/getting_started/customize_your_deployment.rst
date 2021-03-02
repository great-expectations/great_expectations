.. _tutorials__getting_started__customize_your_deployment:

Optional: Customize your deployment
===================================

At this point, you have your first, working local deployment of Great Expectations. You've also been introduced to the foundational concepts in the library: :ref:`data_context`, :ref:`Datasources <reference__core_concepts__datasources>`, :ref:`Expectations`, :ref:`Profilers`, :ref:`Data Docs <reference__core_concepts__data_docs>`, :ref:`Validation`, and :ref:`Checkpoints <reference__core_concepts__validation__checkpoints>`.

Congratulations! You're off to a very good start.

The next step is to customize your deployment by upgrading specific components of your deployment. Data Contexts make this modular, so that you can add or swap out one component at a time. Most of these changes are quick, incremental steps---so you can upgrade from a basic demo deployment to a full production deployment at your own pace and be confident that your Data Context will continue to work at every step along the way.

This last section of this tutorial is designed to present you with clear options for upgrading your deployment. For specific implementation steps, please check out the linked :ref:`how_to_guides`.

Components
--------------------------------------------------

Here's an overview of the components of a typical Great Expectations deployment:

* Great Expectations configs and metadata 

    * :ref:`tutorials__getting_started__customize_your_deployment__options_for_storing_great_expectations_configuration`
    * :ref:`tutorials__getting_started__customize_your_deployment__options_for_storing_expectations`
    * :ref:`tutorials__getting_started__customize_your_deployment__options_for_storing_validation_results`
    * :ref:`tutorials__getting_started__customize_your_deployment__options_for_customizing_generated_notebooks`

* Integrations to related systems

    * :ref:`tutorials__getting_started__customize_your_deployment__additional_datasources_and_generators`
    * :ref:`tutorials__getting_started__customize_your_deployment__options_for_hosting_data_docs`
    * :ref:`tutorials__getting_started__customize_your_deployment__additional_validation_operators_and_actions`
    * :ref:`tutorials__getting_started__customize_your_deployment__options_for_triggering_validation`

..    * Key workflows
..
..        * :ref:`Creating and editing Expectations`
..        * :ref:`Triggering validation`


.. _tutorials__getting_started__customize_your_deployment__options_for_storing_great_expectations_configuration:

Options for storing Great Expectations configuration
--------------------------------------------------------

The simplest way to manage your Great Expectations configuration is usually by committing ``great_expectations/great_expectations.yml`` to git. However, it's not usually a good idea to commit credentials to source control. In some situations, you might need to deploy without access to source control (or maybe even a file system).

Here's how to handle each of those cases:

* :ref:`how_to_guides__configuring_data_contexts__how_to_use_a_yaml_file_or_environment_variables_to_populate_credentials`
* :ref:`how_to_guides__configuring_data_contexts__how_to_populate_credentials_from_a_secrets_store`
* :ref:`how_to_guides__configuring_data_contexts__how_to_instantiate_a_data_context_without_a_yml_file`

.. _tutorials__getting_started__customize_your_deployment__options_for_storing_expectations:

Options for storing Expectations
------------------------------------

Many teams find it convenient to store Expectations in git. Essentially, this approach treats Expectations like test fixtures: they live adjacent to code and are stored within version control. git acts as a collaboration tool and source of record.

Alternatively, you can treat Expectations like configs, and store them in a blob store. Finally, you can store them in a database.

* :ref:`how_to_guides__configuring_metadata_stores__how_to_configure_an_expectation_store_in_amazon_s3`
* :ref:`how_to_guides__configuring_metadata_stores__how_to_configure_an_expectation_store_in_gcs`
* :ref:`how_to_guides__configuring_metadata_stores__how_to_configure_an_expectation_store_in_azure_blob_storage`
* :ref:`how_to_guides__configuring_metadata_stores__how_to_configure_an_expectation_store_to_postgresql`

.. _tutorials__getting_started__customize_your_deployment__options_for_storing_validation_results:

Options for storing Validation Results
------------------------------------------
By default, Validation Results are stored locally, in an uncommitted directory. This is great for individual work, but not good for collaboration. The most common pattern is to use a cloud-based blob store such as S3, GCS, or Azure blob store. You can also store Validation Results in a database.

* :ref:`how_to_guides__configuring_metadata_stores__how_to_configure_a_validation_result_store_on_a_filesystem`
* :ref:`how_to_guides__configuring_metadata_stores__how_to_configure_a_validation_result_store_in_s3`
* :ref:`how_to_guides__configuring_metadata_stores__how_to_configure_a_validation_result_store_in_gcs`
* :ref:`how_to_guides__configuring_metadata_stores__how_to_configure_a_validation_result_store_in_azure_blob_storage`
* :ref:`how_to_guides__configuring_metadata_stores__how_to_configure_a_validation_result_store_to_postgresql`


.. _tutorials__getting_started__customize_your_deployment__options_for_customizing_generated_notebooks:

Options for customizing generated notebooks
-----------------------------------------------
Great Expectations generates and provides notebooks as interactive development environments for expectation suites. You might want to customize parts of the notebooks to add company-specific documentation, or change the code sections to suit your use-cases.

* :ref:`how_to_guides__configuring_generated_notebooks__how_to_configure_suite_edit_generated_notebooks`

.. _tutorials__getting_started__customize_your_deployment__additional_datasources_and_generators:

Additional Datasources
-----------------------

Great Expectations plugs into a wide variety of Datasources, and the list is constantly getting longer. If you have an idea for a Datasource not listed here, please speak up in `the public discussion forum <https://discuss.greatexpectations.io/>`_.

* :ref:`how_to_guides__configuring_datasources__how_to_configure_a_pandas_filesystem_datasource`
* :ref:`how_to_guides__configuring_datasources__how_to_configure_a_pandas_s3_datasource`
* :ref:`how_to_guides__configuring_datasources__how_to_configure_a_redshift_datasource`
* :ref:`how_to_guides__configuring_datasources__how_to_configure_a_snowflake_datasource`
* :ref:`how_to_guides__configuring_datasources__how_to_configure_a_bigquery_datasource`
* :ref:`how_to_guides__configuring_datasources__how_to_configure_a_databricks_azure_datasource`
* :ref:`how_to_guides__configuring_datasources__how_to_configure_an_emr_spark_datasource`
* :ref:`how_to_guides__configuring_datasources__how_to_configure_a_databricks_aws_datasource`
* :ref:`how_to_guides__configuring_datasources__how_to_configure_a_self_managed_spark_datasource`


.. _tutorials__getting_started__customize_your_deployment__options_for_hosting_data_docs:

Options for hosting Data Docs
---------------------------------

By default, Data Docs are stored locally, in an uncommitted directory. This is great for individual work, but not good for collaboration. A better pattern is usually to deploy to a cloud-based blob store (S3, GCS, or Azure blob store), configured to share a static website.

* :ref:`how_to_guides__configuring_data_docs__how_to_host_and_share_data_docs_on_a_filesystem`
* :ref:`how_to_guides__configuring_data_docs__how_to_host_and_share_data_docs_on_s3`
* :ref:`how_to_guides__configuring_data_docs__how_to_host_and_share_data_docs_on_azure_blob_storage`
* :ref:`how_to_guides__configuring_data_docs__how_to_host_and_share_data_docs_on_gcs`


.. _tutorials__getting_started__customize_your_deployment__additional_validation_operators_and_actions:

Additional Validation Operators and Actions
-----------------------------------------------

Most teams will want to configure various :ref:`Validation Actions <validation_actions>` as part of their deployment.

* :ref:`how_to_guides__validation__how_to_update_data_docs_as_a_validation_action`
* :ref:`how_to_guides__validation__how_to_store_validation_results_as_a_validation_action`
* :ref:`how_to_guides__validation__how_to_trigger_slack_notifications_as_a_validation_action`
* :ref:`how_to_guides__validation__how_to_trigger_email_as_a_validation_action`

If you also want to modify your :ref:reference__core_concepts__validation__validation_operator, you can learn how here:

* :ref:`how_to_guides__validation__how_to_add_a_validation_operator`

.. Creating and editing Expectations
.. ---------------------------------
.. 
.. #FIXME: Need words here.
.. 
.. #FIXME: Need list here, after we wrangle the how-to guides for creating and editing Expectations.

.. _tutorials__getting_started__customize_your_deployment__options_for_triggering_validation:

Options for triggering Validation
-------------------------------------

There are two primary patterns for deploying Checkpoints. Sometimes Checkpoints are executed during data processing (e.g. as a task within Airflow). From this vantage point, they can control program flow. Sometimes Checkpoints are executed against materialized data. Great Expectations supports both patterns. There are also some rare instances where you may want to validate data without using a Checkpoint.

* :ref:`how_to_guides__validation__how_to_run_a_checkpoint_in_airflow`
* :ref:`how_to_guides__validation__how_to_run_a_checkpoint_in_python`
* :ref:`how_to_guides__validation__how_to_run_a_checkpoint_in_terminal`
* :ref:`how_to_guides__validation__how_to_validate_data_without_a_checkpoint`
