module.exports = {
  docs: [
    'intro',
    {
      type: 'doc', id: 'tutorials/quickstart/quickstart', label: 'Quickstart'
    },
    {
      type: 'category',
      label: 'Setting up a GX environment',
      link: { type: 'doc', id: 'guides/setup/setup_overview' },
      items: [
        {
          type: 'category',
          label: 'Installation and dependencies',
          items: [
            {
              type: 'html',
              value: '<h4>For use with local filesystems</h4>',
              defaultStyle: true
            },
            'guides/setup/installation/local',
            {
              type: 'html',
              value: '<h4>For use in hosted environments</h4>',
              defaultStyle: true
            },
            'guides/setup/installation/hosted_environment',
            {
              type: 'html',
              value: '<h4>For use with cloud storage</h4>',
              defaultStyle: true
            },
            {
              type: 'doc',
              id: 'guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_on_aws_s3'
            },
            {
              type: 'doc',
              id: 'guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_on_gcs'
            },
            {
              type: 'doc',
              id: 'guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_in_abs'
            },
            {
              type: 'html',
              value: '<h4>For use with SQL Databases</h4>',
              defaultStyle: true
            },
            {
              type: 'doc',
              id: 'guides/setup/optional_dependencies/sql_databases/how_to_setup_gx_to_work_with_sql_databases'
            }
          ]
        },
        {
          type: 'category',
          label: 'Data Contexts',
          items: [
            {
              type: 'html',
              value: '<h4>Quickstart Data Context</h4>',
              defaultStyle: true
            },
            {
              type: 'doc',
              id: 'guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context',
              label: 'How to quickly instantiate a Data Context'
            },
            {
              type: 'html',
              value: '<h4>Filesystem Data Contexts</h4>',
              defaultStyle: true
            },
            'guides/setup/configuring_data_contexts/how_to_configure_a_new_data_context_with_the_cli',
            {
              type: 'doc',
              id: 'guides/setup/configuring_data_contexts/initializing_data_contexts/how_to_initialize_a_filesystem_data_context_in_python',
              label: 'How to initialize a Filesystem Data Context in Python'
            },
            {
              type: 'doc',
              id: 'guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_instantiate_a_specific_filesystem_data_context',
              label: 'How to instantiate a specific Filesystem Data Context'
            },
            {
              type: 'html',
              value: '<h4>In-memory Data Contexts</h4>',
              defaultStyle: true
            },
            'guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_explicitly_instantiate_an_ephemeral_data_context',
            'guides/setup/configuring_data_contexts/how_to_convert_an_ephemeral_data_context_to_a_filesystem_data_context',
            {
              type: 'html',
              value: '<h4>Data Context Configuration</h4>',
              defaultStyle: true
            },
            'guides/setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config',
            'guides/setup/configuring_data_contexts/how_to_configure_credentials'
          ]
        },
        {
          type: 'category',
          label: 'Metadata Stores',
          items: [
            {
              type: 'category',
              label: 'Expectation Stores',
              items: [
                'guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_amazon_s3',
                'guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_azure_blob_storage',
                'guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs',
                'guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_on_a_filesystem',
                'guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_to_postgresql'
              ]
            },
            {
              type: 'category',
              label: 'Validation Result Stores',
              items: [
                'guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_amazon_s3',
                'guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_azure_blob_storage',
                'guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs',
                'guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_on_a_filesystem',
                'guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_to_postgresql'
              ]
            },
            {
              type: 'category',
              label: 'Metric Stores',
              items: [
                'guides/setup/configuring_metadata_stores/how_to_configure_a_metricsstore'
              ]
            }
          ]
        },
        {
          type: 'category',
          label: 'Data Docs',
          items: [
            'guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_a_filesystem',
            'guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage',
            'guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs',
            'guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_amazon_s3'
          ]
        },
        { type: 'doc', id: 'guides/setup/index', label: 'Index' }
      ]
    },
    {
      type: 'category',
      label: 'Connecting to data',
      link: { type: 'doc', id: 'guides/connecting_to_your_data/connect_to_data_overview' },
      items: [
        {
          type: 'category',
          label: 'Filesystem Datasources',
          items: [
            {
              type: 'html',
              value: '<h4>Local Filesystems</h4>',
              defaultStyle: true
            },
            'guides/connecting_to_your_data/fluent/filesystem/how_to_quickly_connect_to_a_single_file_with_pandas',
            'guides/connecting_to_your_data/fluent/filesystem/how_to_connect_to_one_or_more_files_using_pandas',
            'guides/connecting_to_your_data/fluent/filesystem/how_to_connect_to_one_or_more_files_using_spark',
            {
              type: 'html',
              value: '<h4>Google Cloud Storage</h4>',
              defaultStyle: true
            },
            'guides/connecting_to_your_data/fluent/cloud/how_to_connect_to_data_on_gcs_using_pandas',
            'guides/connecting_to_your_data/fluent/cloud/how_to_connect_to_data_on_gcs_using_spark',
            {
              type: 'html',
              value: '<h4>Azure Blob Storage</h4>',
              defaultStyle: true
            },
            'guides/connecting_to_your_data/fluent/cloud/how_to_connect_to_data_on_azure_blob_storage_using_pandas',
            'guides/connecting_to_your_data/fluent/cloud/how_to_connect_to_data_on_azure_blob_storage_using_spark',
            {
              type: 'html',
              value: '<h4>Amazon Web Services S3</h4>',
              defaultStyle: true
            },
            'guides/connecting_to_your_data/fluent/cloud/how_to_connect_to_data_on_s3_using_pandas',
            'guides/connecting_to_your_data/fluent/cloud/how_to_connect_to_data_on_s3_using_spark'
          ]
        },
        {
          type: 'category',
          label: 'In-memory Datasources',
          items: [
            'guides/connecting_to_your_data/fluent/in_memory/how_to_connect_to_in_memory_data_using_pandas'
          ]
        },
        {
          type: 'category',
          label: 'SQL Datasources',
          items: [
            {
              type: 'html',
              value: '<h4>General SQL Datasources</h4>',
              defaultStyle: true
            },
            'guides/connecting_to_your_data/fluent/database/how_to_connect_to_sql_data',
            {
              type: 'html',
              value: '<h4>Specific SQL dialects</h4>',
              defaultStyle: true
            },
            'guides/connecting_to_your_data/fluent/database/how_to_connect_to_postgresql_data',
            'guides/connecting_to_your_data/fluent/database/how_to_connect_to_sqlite_data'
          ]
        },
        {
          type: 'category',
          label: 'Working with Data Assets',
          items: [
            {
              type: 'html',
              value: '<h4>All Data Assets</h4>',
              defaultStyle: true
            },
            'guides/connecting_to_your_data/fluent/batch_requests/how_to_request_data_from_a_data_asset',
            {
              type: 'html',
              value: '<h4>Filesystem Data Assets</h4>',
              defaultStyle: true
            },
            'guides/connecting_to_your_data/fluent/data_assets/how_to_organize_batches_in_a_file_based_data_asset',
            {
              type: 'html',
              value: '<h4>SQL Data Assets</h4>',
              defaultStyle: true
            },
            'guides/connecting_to_your_data/fluent/database/how_to_connect_to_a_sql_table',
            'guides/connecting_to_your_data/fluent/database/how_to_connect_to_sql_data_using_a_query',
            'guides/connecting_to_your_data/fluent/data_assets/how_to_organize_batches_in_a_sql_based_data_asset'
          ]
        },
        {
          type: 'category',
          label: 'Advanced Datasource configuration',
          items: [
            {
              type: 'category',
              label: 'Filesystem block-configuration',
              items: [
                'guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_pandas_datasource',
                'guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_spark_datasource'
              ]
            },
            {
              type: 'category',
              label: 'In-memory block-configuration',
              items: [
                'guides/connecting_to_your_data/in_memory/pandas',
                'guides/connecting_to_your_data/in_memory/spark'
              ]
            },
            {
              type: 'category',
              label: 'Cloud block-configuration',
              items: [
                'guides/connecting_to_your_data/cloud/s3/pandas',
                'guides/connecting_to_your_data/cloud/s3/spark',
                'guides/connecting_to_your_data/cloud/gcs/pandas',
                'guides/connecting_to_your_data/cloud/gcs/spark',
                'guides/connecting_to_your_data/cloud/azure/pandas',
                'guides/connecting_to_your_data/cloud/azure/spark'
              ]
            },
            {
              type: 'category',
              label: 'SQL database block-configuration',
              items: [
                'guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_sql_datasource',
                'guides/connecting_to_your_data/database/athena',
                'guides/connecting_to_your_data/database/bigquery',
                'guides/connecting_to_your_data/database/mssql',
                'guides/connecting_to_your_data/database/mysql',
                'guides/connecting_to_your_data/database/postgres',
                'guides/connecting_to_your_data/database/redshift',
                'guides/connecting_to_your_data/database/snowflake',
                'guides/connecting_to_your_data/database/sqlite',
                'guides/connecting_to_your_data/database/trino'
              ]
            },
            {
              type: 'category',
              label: 'Block-configuration compatible Batch Requests',
              items: [
                'guides/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource',
                'guides/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_or_pandas_dataframe'
              ]
            }
          ]
        },
        { type: 'doc', id: 'guides/connecting_to_your_data/index', label: 'Index' }
      ]
    },
    {
      type: 'category',
      label: 'Creating Expectations',
      link: { type: 'doc', id: 'guides/expectations/create_expectations_overview' },
      items: [
        {
          type: 'category',
          label: 'Core skills',
          items: [
            'guides/expectations/how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly',
            'guides/expectations/how_to_create_and_edit_expectations_with_a_profiler',
            'guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data',
            { type: 'doc', id: 'guides/expectations/how_to_use_auto_initializing_expectations' }
          ]
        },
        {
          type: 'category',
          label: 'Profilers and Data Assistants',
          items: [
            'guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant',
            'guides/expectations/advanced/how_to_create_a_new_expectation_suite_using_rule_based_profilers'
          ]
        },
        {
          type: 'category',
          label: 'Advanced skills',
          items: [
            'guides/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters',
            'guides/expectations/advanced/how_to_dynamically_load_evaluation_parameters_from_a_database',
            'guides/expectations/advanced/how_to_compare_two_tables_with_the_onboarding_data_assistant'
          ]
        },
        {
          type: 'category',
          label: 'Creating Custom Expectations',
          items: [
            'guides/expectations/creating_custom_expectations/overview',
            'guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations',
            'guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations',
            'guides/expectations/creating_custom_expectations/how_to_create_custom_batch_expectations',
            'guides/expectations/creating_custom_expectations/how_to_create_custom_column_pair_map_expectations',
            'guides/expectations/creating_custom_expectations/how_to_create_custom_multicolumn_map_expectations',
            'guides/expectations/creating_custom_expectations/how_to_create_custom_regex_based_column_map_expectations',
            'guides/expectations/creating_custom_expectations/how_to_create_custom_set_based_column_map_expectations',
            'guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations',
            'guides/expectations/creating_custom_expectations/how_to_create_custom_parameterized_expectations',
            'guides/expectations/creating_custom_expectations/how_to_add_support_for_the_auto_initializing_framework_to_a_custom_expectation',
            'guides/expectations/creating_custom_expectations/how_to_use_custom_expectations',
            {
              type: 'category',
              label: 'Adding Features to Custom Expectations',
              items: [
                'guides/expectations/advanced/how_to_add_comments_to_expectations_and_display_them_in_data_docs',
                'guides/expectations/features_custom_expectations/how_to_add_example_cases_for_an_expectation',
                'guides/expectations/features_custom_expectations/how_to_add_input_validation_for_an_expectation',
                'guides/expectations/features_custom_expectations/how_to_add_spark_support_for_an_expectation',
                'guides/expectations/features_custom_expectations/how_to_add_sqlalchemy_support_for_an_expectation'
              ]
            }
          ]
        },
        { type: 'doc', id: 'guides/expectations/index', label: 'Index' }
      ]
    },
    {
      type: 'category',
      label: 'Validating data',
      link: { type: 'doc', id: 'guides/validation/validate_data_overview' },
      items: [
        {
          type: 'category',
          label: 'Core skills',
          items: [
            'guides/validation/how_to_validate_data_by_running_a_checkpoint'
          ]
        },
        {
          type: 'category',
          label: 'Checkpoints',
          items: [
            'guides/validation/checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint',
            'guides/validation/checkpoints/how_to_create_a_new_checkpoint',
            'guides/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config',
            'guides/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint'
          ]
        },
        {
          type: 'category',
          label: 'Actions',
          items: [
            'guides/validation/validation_actions/how_to_trigger_email_as_a_validation_action',
            'guides/validation/validation_actions/how_to_collect_openlineage_metadata_using_a_validation_action',
            'guides/validation/validation_actions/how_to_trigger_opsgenie_notifications_as_a_validation_action',
            'guides/validation/validation_actions/how_to_trigger_slack_notifications_as_a_validation_action',
            'guides/validation/validation_actions/how_to_update_data_docs_as_a_validation_action'
          ]
        },
        {
          type: 'category',
          label: 'Advanced',
          items: [
            'guides/validation/advanced/how_to_deploy_a_scheduled_checkpoint_with_cron',
            'guides/validation/advanced/how_to_get_data_docs_urls_for_custom_validation_actions',
            'guides/validation/advanced/how_to_validate_data_without_a_checkpoint',
            'guides/validation/advanced/how_to_validate_data_with_an_in_memory_checkpoint'
          ]
        },
        { type: 'doc', id: 'guides/validation/index', label: 'Index' }
      ]
    },
    {
      type: 'category',
      label: 'Integration guides',
      link: { type: 'doc', id: 'deployment_patterns/integrations_and_howtos_overview' },
      items: [
        {
          type: 'category',
          label: 'Using Great Expectations with AWS',
          items: [
            'deployment_patterns/how_to_use_great_expectations_in_aws_glue',
            { label: 'How to use Great Expectations with AWS using S3 and Pandas', type: 'doc', id: 'deployment_patterns/how_to_use_gx_with_aws/how_to_use_gx_with_aws_using_cloud_storage_and_pandas' },
            { label: 'How to use Great Expectations with AWS using S3 and Spark', type: 'doc', id: 'deployment_patterns/how_to_use_gx_with_aws/how_to_use_gx_with_aws_using_s3_and_spark' },
            { label: 'How to use Great Expectations with AWS using Athena', type: 'doc', id: 'deployment_patterns/how_to_use_gx_with_aws/how_to_use_gx_with_aws_using_athena' },
            { label: 'How to use Great Expectations with AWS using Redshift', type: 'doc', id: 'deployment_patterns/how_to_use_gx_with_aws/how_to_use_gx_with_aws_using_redshift' }
          ]
        },
        'deployment_patterns/how_to_instantiate_a_data_context_hosted_environments',
        'deployment_patterns/how_to_instantiate_a_data_context_on_an_emr_spark_cluster',
        'deployment_patterns/how_to_use_great_expectations_with_airflow',
        'deployment_patterns/how_to_use_great_expectations_in_databricks',
        { type: 'doc', id: 'integrations/integration_datahub' },
        'deployment_patterns/how_to_use_great_expectations_in_deepnote',
        'deployment_patterns/how_to_use_great_expectations_in_flyte',
        'deployment_patterns/how_to_use_great_expectations_with_google_cloud_platform_and_bigquery',
        'deployment_patterns/how_to_use_great_expectations_with_meltano',
        'deployment_patterns/how_to_use_great_expectations_with_prefect',
        'deployment_patterns/how_to_use_great_expectations_with_ydata_synthetic',
        'deployment_patterns/how_to_use_great_expectations_in_emr_serverless',
        { type: 'doc', id: 'integrations/integration_zenml' },
        { type: 'doc', id: 'deployment_patterns/index', label: 'Index' }
      ]
    },
    {
      type: 'category',
      label: 'Contributing',
      link: { type: 'doc', id: 'contributing/contributing' },
      items: [
        {
          type: 'category',
          label: 'Contributing basics',
          items: [
            { type: 'doc', id: 'contributing/contributing_setup' },
            { type: 'doc', id: 'contributing/contributing_checklist' },
            { type: 'doc', id: 'contributing/contributing_github' },
            { type: 'doc', id: 'contributing/contributing_test' },
            { type: 'doc', id: 'contributing/contributing_maturity' },
            { type: 'doc', id: 'contributing/contributing_misc' }
          ]
        },
        {
          type: 'category',
          label: 'Contributing specifics',
          items: [
            {
              type: 'category',
              label: 'How to contribute how-to guides',
              items: [
                { type: 'doc', id: 'guides/miscellaneous/how_to_write_a_how_to_guide' },
                { type: 'doc', id: 'guides/miscellaneous/how_to_template' }
              ]
            },
            {
              type: 'category',
              label: 'How to contribute integration documentation',
              items: [
                'integrations/contributing_integration',
                { type: 'doc', id: 'integrations/integration_template', label: 'TEMPLATE Integration Document' }
              ]
            },
            { type: 'doc', id: 'guides/expectations/contributing/how_to_contribute_a_custom_expectation_to_great_expectations' },
            { type: 'doc', id: 'contributing/contributing_package' }
          ]
        },
        {
          type: 'category',
          label: 'Style guides',
          items: [
            { type: 'doc', id: 'contributing/style_guides/docs_style' },
            { type: 'doc', id: 'contributing/style_guides/code_style' },
            { type: 'doc', id: 'contributing/style_guides/cli_and_notebooks_style' }
          ]
        },
        'contributing/index'
      ]
    },
    {
      type: 'category',
      label: 'Reference',
      link: { type: 'doc', id: 'reference/reference_overview' },
      items: [
        {
          type: 'category',
          label: 'Supplemental documentation',
          link: { type: 'doc', id: 'reference/supplemental_documentation' },
          items: [
            { type: 'doc', id: 'guides/miscellaneous/how_to_use_the_great_expectations_cli' },
            { type: 'doc', id: 'guides/miscellaneous/how_to_use_the_project_check_config_command' },
            { type: 'doc', id: 'reference/customize_your_deployment' },
            { type: 'doc', id: 'reference/usage_statistics' }
          ]
        },
        {
          type: 'category',
          label: 'API documentation',
          link: { type: 'doc', id: 'reference/api_reference' },
          items: [
            {
              type: 'autogenerated',
              dirName: 'reference/api'
            }
          ]
        },
        {
          type: 'category',
          label: 'Glossary of Terms',
          link: { type: 'doc', id: 'glossary' },
          items: [
            'terms/action',
            'terms/batch',
            'terms/batch_request',
            'terms/custom_expectation',
            'terms/checkpoint',
            'terms/cli',
            'terms/datasource',
            'terms/data_context',
            'terms/data_asset',
            'terms/data_assistant',
            'terms/data_connector',
            'terms/data_docs',
            'terms/evaluation_parameter',
            'terms/execution_engine',
            {
              type: 'category',
              label: 'Expectations',
              link: { type: 'doc', id: 'terms/expectation' },
              collapsed: true,
              items: [
                { type: 'doc', id: 'reference/expectations/conditional_expectations' },
                { type: 'doc', id: 'reference/expectations/distributional_expectations' },
                { type: 'doc', id: 'reference/expectations/implemented_expectations' },
                { type: 'doc', id: 'reference/expectation_suite_operations' },
                { type: 'doc', id: 'reference/expectations/result_format' },
                { type: 'doc', id: 'reference/expectations/standard_arguments' }
              ]
            },
            'terms/expectation_suite',
            'terms/metric',
            'terms/plugin',
            'terms/profiler',
            {
              type: 'category',
              label: 'Stores',
              link: { type: 'doc', id: 'terms/store' },
              items: [
                'terms/checkpoint_store',
                'terms/data_docs_store',
                'terms/evaluation_parameter_store',
                'terms/expectation_store',
                'terms/metric_store',
                'terms/validation_result_store'
              ]
            },
            'terms/renderer',
            'terms/supporting_resource',
            'terms/validator',
            'terms/validation_result'
          ]
        }
      ]
    },
    { type: 'doc', id: 'changelog' },
    { type: 'doc', id: 'guides/miscellaneous/migration_guide' }
  ]
}
