module.exports = {
  docs: [
    'intro',
    {
      type: 'category',
      label: 'Getting Started (A Tutorial)',
      items: [
        { type: 'doc', id: 'tutorials/getting_started/tutorial_overview', label: 'Overview' },
        { type: 'doc', id: 'tutorials/getting_started/tutorial_setup', label: '1. Setup' },
        { type: 'doc', id: 'tutorials/getting_started/tutorial_connect_to_data', label: '2. Connect to Data' },
        { type: 'doc', id: 'tutorials/getting_started/tutorial_create_expectations', label: '3. Create Expectations' },
        { type: 'doc', id: 'tutorials/getting_started/tutorial_validate_data', label: '4. Validate Data' },
        { type: 'doc', id: 'tutorials/getting_started/tutorial_review', label: 'Review and next steps' }
        // { type: 'doc', id: 'tutorials/getting_started/check_out_data_docs', label: 'Viewing your Results' },
        // { type: 'doc', id: 'tutorials/getting_started/customize_your_deployment', label: 'Next Steps: Customizing for your Deployment' }
      ]
    },
    // {
    //   type: 'category',
    //   label: 'Getting Started',
    //   items: [
    //     'tutorials/getting_started/intro',
    //     'tutorials/getting_started/initialize_a_data_context',
    //     'tutorials/getting_started/connect_to_data',
    //     'tutorials/getting_started/create_your_first_expectations',
    //     'tutorials/getting_started/check_out_data_docs',
    //     'tutorials/getting_started/validate_your_data',
    //     'tutorials/getting_started/customize_your_deployment'
    //   ]
    // },
    {
      type: 'category',
      label: 'Step 1: Setup',
      items: [
        { type: 'doc', id: 'guides/setup/setup_overview', label: 'Overview' },
        {
          type: 'category',
          label: 'How to guides',
          items: [
            {
              type: 'category',
              label: 'Installation',
              items: [
                'guides/setup/installation/local',
                'guides/setup/installation/hosted_environment',
                'guides/setup/installation/index'
              ]
            },
            {
              type: 'category',
              label: 'Data Contexts',
              items: [
                'guides/setup/configuring_data_contexts/how_to_configure_a_new_data_context_with_the_cli',
                'guides/setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config',
                'guides/setup/configuring_data_contexts/how_to_configure_credentials',
                'guides/setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file',
                'guides/setup/configuring_data_contexts/index'
              ]
            },
            {
              type: 'category',
              label: 'Stores',
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
                },
                'guides/setup/configuring_metadata_stores/index'
              ]
            },
            {
              type: 'category',
              label: 'Data Docs',
              items: [
                'guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_a_filesystem',
                'guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage',
                'guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs',
                'guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_amazon_s3',
                'guides/setup/configuring_data_docs/index'
              ]
            },
            {
              type: 'category',
              label: 'Advanced',
              items: [
                { type: 'doc', id: 'guides/miscellaneous/how_to_use_the_project_check_config_command' },
                { type: 'doc', id: 'guides/miscellaneous/how_to_use_the_great_expectation_docker_images' }
              ]
            },
            { type: 'doc', id: 'guides/setup/index', label: 'Index' }
          ]
        }
      ]
    },
    {
      type: 'category',
      label: 'Step 2: Connect To Data',
      items: [
        { type: 'doc', id: 'guides/connecting_to_your_data/connect_to_data_overview', label: 'Overview' },
        {
          type: 'category',
          label: 'How to guides',
          items: [
            {
              type: 'category',
              label: 'Core skills',
              items: [
                'guides/connecting_to_your_data/how_to_choose_which_dataconnector_to_use',
                'guides/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector',
                'guides/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector',
                'guides/connecting_to_your_data/how_to_configure_a_runtimedataconnector',
                'guides/connecting_to_your_data/how_to_configure_a_dataconnector_to_introspect_and_partition_a_file_system_or_blob_store',
                'guides/connecting_to_your_data/how_to_configure_a_dataconnector_to_introspect_and_partition_tables_in_sql',
                'guides/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_or_pandas_dataframe',
                'guides/connecting_to_your_data/how_to_get_a_batch_of_data_from_a_configured_datasource'
              ]
            },
            {
              type: 'category',
              label: 'In memory',
              items: [
                'guides/connecting_to_your_data/in_memory/pandas',
                'guides/connecting_to_your_data/in_memory/spark'
              ]
            },
            {
              type: 'category',
              label: 'Database',
              items: [
                'guides/connecting_to_your_data/database/athena',
                'guides/connecting_to_your_data/database/bigquery',
                'guides/connecting_to_your_data/database/mssql',
                'guides/connecting_to_your_data/database/mysql',
                'guides/connecting_to_your_data/database/postgres',
                'guides/connecting_to_your_data/database/redshift',
                'guides/connecting_to_your_data/database/snowflake',
                'guides/connecting_to_your_data/database/sqlite'
              ]
            },
            {
              type: 'category',
              label: 'Filesystem',
              items: [
                'guides/connecting_to_your_data/filesystem/pandas',
                'guides/connecting_to_your_data/filesystem/spark'
              ]
            },
            {
              type: 'category',
              label: 'Cloud',
              items: [
                'guides/connecting_to_your_data/cloud/s3/pandas',
                'guides/connecting_to_your_data/cloud/s3/spark',
                'guides/connecting_to_your_data/cloud/gcs/pandas',
                'guides/connecting_to_your_data/cloud/gcs/spark',
                'guides/connecting_to_your_data/cloud/azure/pandas',
                'guides/connecting_to_your_data/cloud/azure/spark'
              ]
            },
            // {
            // type: 'category',
            // label: 'Contributing',
            // items: [
            // 'guides/connecting_to_your_data/contributing/how_to_add_support_for_a_new_sqlalchemy_dialect'
            // ]
            // },
            {
              type: 'category',
              label: 'Advanced',
              items: [
                'guides/connecting_to_your_data/advanced/how_to_configure_a_dataconnector_for_splitting_and_sampling_a_file_system_or_blob_store',
                'guides/connecting_to_your_data/advanced/how_to_configure_a_dataconnector_for_splitting_and_sampling_tables_in_sql'
                // 'guides/connecting_to_your_data/advanced/how_to_create_a_batch_from_a_sql_query',
                // 'guides/connecting_to_your_data/advanced/how_to_create_a_lightweight_data_catalog_by_applying_a_descriptive_profiler_to_a_configured_datasource',
                // 'guides/connecting_to_your_data/advanced/how_to_explore_changes_in_data_over_time_using_a_configured_datasource'
              ]
            },
            { type: 'doc', id: 'guides/connecting_to_your_data/index', label: 'Index' }
          ]
        }
      ]
    },
    {
      type: 'category',
      label: 'Step 3: Create Expectations',
      items: [
        { type: 'doc', id: 'guides/expectations/create_expectations_overview', label: 'Overview' },
        {
          type: 'category',
          label: 'How to guides',
          items: [
            {
              type: 'category',
              label: 'Core skills',
              items: [
                'guides/expectations/how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly',
                'guides/expectations/how_to_create_and_edit_expectations_in_bulk',
                'guides/expectations/how_to_create_and_edit_expectations_with_a_profiler',
                'guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data',
                { type: 'doc', id: 'guides/miscellaneous/how_to_quickly_explore_expectations_in_a_notebook' },
                { type: 'doc', id: 'guides/miscellaneous/how_to_configure_notebooks_generated_by_suite_edit' }
              ]
            },
            {
              type: 'category',
              label: 'Configuring Profilers',
              items: []
            },
            {
              type: 'category',
              label: 'Creating Custom Expectations',
              items: [
                'guides/expectations/creating_custom_expectations/how_to_create_custom_expectations',
                // 'guides/expectations/creating_custom_expectations/how_to_create_custom_expectations_from_a_sql_query',
                'guides/expectations/creating_custom_expectations/how_to_create_custom_parameterized_expectations'
              ]
            },
            {
              type: 'category',
              label: 'Advanced',
              items: [
                'guides/expectations/advanced/how_to_add_comments_to_expectations_and_display_them_in_data_docs',
                'guides/expectations/advanced/how_to_create_renderers_for_custom_expectations',
                'guides/expectations/advanced/how_to_create_a_new_expectation_suite_by_profiling_from_a_jsonschema_file',
                'guides/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters',
                'guides/expectations/advanced/how_to_dynamically_load_evaluation_parameters_from_a_database',
                'guides/expectations/advanced/how_to_create_a_new_expectation_suite_using_rule_based_profilers'
              ]
            },
            { type: 'doc', id: 'guides/expectations/index', label: 'Index' }
          ]
        }
      ]
    },
    {
      type: 'category',
      label: 'Step 4: Validate Data',
      items: [
        { type: 'doc', id: 'guides/validation/validate_data_overview', label: 'Overview' },
        {
          type: 'category',
          label: 'How to guides',
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
              label: 'Validation Actions',
              items: [
                // 'guides/validation/validation_actions/how_to_store_validation_results_as_a_validation_action',
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
                'guides/validation/advanced/how_to_implement_custom_notifications',
                'guides/validation/advanced/how_to_validate_data_without_a_checkpoint'
              ]
            },
            { type: 'doc', id: 'guides/validation/index', label: 'Index' }
          ]
        }
      ]
    },
    {
      type: 'category',
      label: 'Deployment Patterns',
      items: [
        'deployment_patterns/how_to_instantiate_a_data_context_hosted_environments',
        'deployment_patterns/how_to_use_great_expectations_in_databricks',
        'deployment_patterns/how_to_use_great_expectations_with_google_cloud_platform_and_bigquery',
        'deployment_patterns/how_to_instantiate_a_data_context_on_an_emr_spark_cluster',
        'deployment_patterns/how_to_use_great_expectations_with_airflow',
        'deployment_patterns/how_to_use_great_expectations_in_flyte',
        { type: 'doc', id: 'deployment_patterns/index', label: 'Index' }
        // {
        // type: 'category',
        // label: 'Contributing',
        // items: [
        // 'deployment_patterns/contributing/how_to_add_a_new_deployment_pattern_document',
        // 'deployment_patterns/contributing/how_to_contribute_to_an_existing_deployment_pattern_document'
        // ]
        // }
      ]
    },
    {
      type: 'category',
      label: 'Contributing',
      collapsed: true,
      items: [
        { type: 'doc', id: 'contributing/contributing', label: 'Introduction' },
        { type: 'doc', id: 'contributing/contributing_setup' },
        { type: 'doc', id: 'contributing/contributing_checklist' },
        { type: 'doc', id: 'contributing/contributing_github' },
        { type: 'doc', id: 'contributing/contributing_test' },
        { type: 'doc', id: 'contributing/contributing_maturity' },
        { type: 'doc', id: 'contributing/contributing_misc' },
        'guides/expectations/contributing/how_to_contribute_a_new_expectation_to_great_expectations',
        {
          type: 'category',
          label: 'Style guides',
          items: [
            { type: 'doc', id: 'contributing/style_guides/docs_style' },
            { type: 'doc', id: 'contributing/style_guides/code_style' },
            { type: 'doc', id: 'contributing/style_guides/cli_and_notebooks_style' },
            { type: 'doc', id: 'guides/miscellaneous/how_to_write_a_how_to_guide' },
            { type: 'doc', id: 'guides/miscellaneous/how_to_template' }
          ]
        }
      ]
    },
    {
      type: 'category',
      label: 'Reference',
      items: [
        {
          type: 'doc',
          label: 'API Reference',
          id: 'reference/api_reference'
        },
        'glossary',
        {
          type: 'category',
          label: 'Technical Terms',
          items: [
            'terms/action',
            'terms/batch',
            'terms/batch_request',
            'terms/datasource',
            'terms/data_context',
            'terms/data_asset',
            'terms/data_docs',
            'terms/checkpoint',
            'terms/execution_engine',
            'terms/metric',
            'terms/plugin',
            'terms/profiler',
            'terms/expectation',
            'terms/expectation_suite',
            'terms/plugin',
            'terms/supporting_resource',
            'terms/validation_result'
          ]
        },
        {
          type: 'category',
          label: 'Supplemental Documentation',
          items: [
            { type: 'doc', id: 'guides/miscellaneous/how_to_use_the_great_expectations_cli' },
            { type: 'doc', id: 'reference/anonymous_usage_statistics' },
            { type: 'doc', id: 'reference/supporting_resources' },
            {
              type: 'category',
              label: 'Expectations',
              collapsed: true,
              items: [
                { type: 'doc', id: 'reference/expectations/conditional_expectations' },
                { type: 'doc', id: 'reference/expectations/distributional_expectations' },
                { type: 'doc', id: 'reference/expectations/implemented_expectations' },
                { type: 'doc', id: 'reference/expectations/standard_arguments' },
                { type: 'doc', id: 'reference/expectations/result_format' }
              ]
            }
          ]
        },
        {
          type: 'category',
          label: 'Core Concepts',
          items: [
            { type: 'doc', id: 'reference/core_concepts', label: 'Introduction' },
            { type: 'doc', id: 'reference/checkpoints_and_actions' },
            { type: 'doc', id: 'reference/data_context' },
            { type: 'doc', id: 'reference/data_discovery' },
            { type: 'doc', id: 'reference/data_docs' },
            { type: 'doc', id: 'reference/datasources' },
            { type: 'doc', id: 'reference/evaluation_parameters' },
            { type: 'doc', id: 'reference/execution_engine' },
            { type: 'doc', id: 'reference/metrics' },
            { type: 'doc', id: 'reference/profilers' },
            // { type: 'doc', id: 'reference/stores' },
            { type: 'doc', id: 'reference/dividing_data_assets_into_batches' },
            { type: 'doc', id: 'reference/validation' },
            { type: 'doc', id: 'reference/anonymous_usage_statistics' },
            { type: 'doc', id: 'reference/supporting_resources' }
          ]
        }
      ]
    },
    {
      type: 'category',
      label: 'Updates and Migration',
      items: [
        { type: 'doc', id: 'changelog' },
        { type: 'doc', id: 'guides/miscellaneous/migration_guide' }
      ]
    }
  ]
}
