module.exports = {
  docs: [
    'intro',
    {
      type: 'category',
      label: 'Get started with GX',
      link: { type: 'doc', id: 'guides/setup/get_started_lp'},
      items: [
        'tutorials/quickstart/quickstart',
        'tutorials/getting_started/how_to_use_great_expectations_in_databricks',
        'tutorials/getting_started/how_to_use_great_expectations_with_sql',
      ]
    },
    {
      type: 'category',
      label: 'Configure your GX environment',
      link: { type: 'doc', id: 'guides/setup/setup_overview_lp' },
      items: [
        'guides/setup/setup_overview',
        'guides/setup/installation/install_gx',
        {
          type: 'category',
          label: 'Configure Data Contexts',
          link: { type: 'doc', id: 'guides/setup/configure_data_contexts_lp' },
          items: [
            'guides/setup/configuring_data_contexts/instantiating_data_contexts/instantiate_data_context',
            'guides/setup/configuring_data_contexts/how_to_convert_an_ephemeral_data_context_to_a_filesystem_data_context',
            'guides/setup/configuring_data_contexts/how_to_configure_credentials',
          ]
        },
        'guides/setup/configuring_metadata_stores/configure_expectation_stores',
        'guides/setup/configuring_metadata_stores/configure_result_stores',
        'guides/setup/configuring_metadata_stores/how_to_configure_a_metricsstore',
        'guides/setup/configuring_data_docs/host_and_share_data_docs',
      ]
    },
    {
      type: 'category',
      label: 'Connect to Source Data',
      link: { type: 'doc', id: 'guides/connecting_to_your_data/connect_to_data_lp' },
      items: [
        'guides/connecting_to_your_data/fluent/filesystem/connect_filesystem_source_data',
        'guides/connecting_to_your_data/fluent/in_memory/how_to_connect_to_in_memory_data_using_pandas',
        'guides/connecting_to_your_data/fluent/database/connect_sql_source_data',
        {
          type: 'category',
          label: 'Manage Data Assets',
          link: { type: 'doc', id: 'guides/connecting_to_your_data/manage_data_assets_lp' },
          items: [
            'guides/connecting_to_your_data/fluent/batch_requests/how_to_request_data_from_a_data_asset',
            'guides/connecting_to_your_data/fluent/data_assets/how_to_organize_batches_in_a_file_based_data_asset',
            'guides/connecting_to_your_data/fluent/database/sql_data_assets',
          ]
        },
      ]
    },
    {
      type: 'category',
      label: 'Create Expectations',
      link: { type: 'doc', id: 'guides/expectations/expectations_lp' },
      items: [
        'guides/expectations/create_expectations_overview',
        {
          type: 'category',
          label: 'Manage Expectations and Expectation Suites',
          link: { type: 'doc', id: 'guides/expectations/create_manage_expectations_lp' },
          items: [
            'guides/expectations/how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly',
            'guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data',
            'guides/expectations/how_to_edit_an_existing_expectationsuite',
            'guides/expectations/how_to_use_auto_initializing_expectations',
            'guides/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters',
            'guides/expectations/advanced/how_to_dynamically_load_evaluation_parameters_from_a_database',
            'guides/expectations/advanced/how_to_compare_two_tables_with_the_onboarding_data_assistant',
          ]
        },
        {
          type: 'category',
          label: 'Profilers and Data Assistants',
          link: { type: 'doc', id: 'guides/expectations/profilers_data_assistants_lp' },
          items: [
            'guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant',
            'guides/expectations/advanced/how_to_create_a_new_expectation_suite_using_rule_based_profilers',
          ]
        },
        {
          type: 'category',
          label: 'Create Custom Expectations',
          link: { type: 'doc', id: 'guides/expectations/custom_expectations_lp' },
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
          ]
        },
        'guides/expectations/creating_custom_expectations/how_to_use_custom_expectations',
        {
          type: 'category',
          label: 'Add Features to Custom Expectations',
          link: { type: 'doc', id: 'guides/expectations/add_features_custom_expectations_lp' },
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
    {
      type: 'category',
      label: 'Validate Data',
      link: { type: 'doc', id: 'guides/validation/validate_data_lp' },
      items: [
        'guides/validation/validate_data_overview',
        {
          type: 'category',
          label: 'Manage Checkpoints',
          link: { type: 'doc', id: 'guides/validation/checkpoints/checkpoint_lp' },
          items: [
            'guides/validation/checkpoints/how_to_create_a_new_checkpoint',
            'guides/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config',
            'guides/validation/checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint',
            'guides/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint',
            'guides/validation/advanced/how_to_deploy_a_scheduled_checkpoint_with_cron',
          ]
        },
        {
          type: 'category',
          label: 'Configure Actions',
          link: { type: 'doc', id: 'guides/validation/validation_actions/actions_lp' },
          items: [
            'guides/validation/validation_actions/how_to_trigger_email_as_a_validation_action',
            'guides/validation/validation_actions/how_to_collect_openlineage_metadata_using_a_validation_action',
            'guides/validation/validation_actions/how_to_trigger_opsgenie_notifications_as_a_validation_action',
            'guides/validation/validation_actions/how_to_trigger_slack_notifications_as_a_validation_action',
            'guides/validation/validation_actions/how_to_update_data_docs_as_a_validation_action',
            'guides/validation/advanced/how_to_get_data_docs_urls_for_custom_validation_actions',
          ]
        },
      ]
    },
    {
      type: 'category',
      label: 'Integrations',
      link: {
        type: 'generated-index',
        title: 'Integrations',
        description: 'Integrate Great Expectations (GX) with commonly used data engineering tools.',
      },
      items: [
        {
          type: 'category',
          label: 'Amazon Web Services (AWS)',
          link: {
            type: 'doc',
            id: 'deployment_patterns/aws_lp',
          },
          items: [
            'deployment_patterns/how_to_use_great_expectations_in_aws_glue',
            'deployment_patterns/how_to_instantiate_a_data_context_on_an_emr_spark_cluster',
            'deployment_patterns/how_to_use_great_expectations_in_emr_serverless',
            'deployment_patterns/how_to_use_gx_with_aws/how_to_use_gx_with_aws_using_cloud_storage_and_pandas',
            'deployment_patterns/how_to_use_gx_with_aws/how_to_use_gx_with_aws_using_s3_and_spark',
            'deployment_patterns/how_to_use_gx_with_aws/how_to_use_gx_with_aws_using_athena',
            'deployment_patterns/how_to_use_gx_with_aws/how_to_use_gx_with_aws_using_redshift',
          ],
        },
        'deployment_patterns/how_to_instantiate_a_data_context_hosted_environments',
        'deployment_patterns/how_to_use_great_expectations_with_airflow',
        'integrations/integration_datahub',
        'deployment_patterns/how_to_use_great_expectations_in_deepnote',
        'deployment_patterns/how_to_use_great_expectations_in_flyte',
        'deployment_patterns/how_to_use_great_expectations_with_google_cloud_platform_and_bigquery',
        'deployment_patterns/how_to_use_great_expectations_with_meltano',
        'deployment_patterns/how_to_use_great_expectations_with_prefect',
        'deployment_patterns/how_to_use_great_expectations_with_ydata_synthetic',
        'integrations/integration_zenml',
      ]
    },
    {
      type: 'category',
      label: 'Reference',
      link: { type: 'doc', id: 'reference/reference_overview' },
      items: [
    'contributing/contributing_maturity',
    'reference/customize_your_deployment',
    'reference/usage_statistics',
    'conceptual_guides/expectation_classes',
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
    { type: 'doc', id: 'guides/miscellaneous/migration_guide' },
    'contributing/contributing',
  ]
}
