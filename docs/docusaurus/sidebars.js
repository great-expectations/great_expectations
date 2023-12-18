module.exports = {
  gx_cloud: [
        {
          type: 'category',
          label: 'About GX Cloud',
          link: { type: 'doc', id: 'cloud/about_gx' },
          items: [
            {
              type: 'link',
              label: 'GX Cloud architecture',
              href: '/docs/cloud/about_gx#gx-cloud-architecture',
            },
            {
              type: 'link',
              label: 'GX Cloud deployment patterns',
              href: '/docs/cloud/about_gx#gx-cloud-deployment-patterns',
            },
            {
              type: 'link',
              label: 'Workflow optimization',
              href: '/docs/cloud/about_gx#workflow-optimization',
            },
            {
              type: 'link',
              label: 'GX Cloud workflow',
              href: '/docs/cloud/about_gx#gx-cloud-workflow',
            },
            {
              type: 'link',
              label: 'Roles and responsibilities',
              href: '/docs/cloud/about_gx#roles-and-responsibilities',
            },
            {
              type: 'link',
              label: 'Supported browsers',
              href: '/docs/cloud/about_gx#supported-browsers',
            },
            {
              type: 'link',
              label: 'Get support',
              href: '/docs/cloud/about_gx#get-support',
            },
          ]
        },
        {
          type: 'category',
          label: 'Set up GX Cloud',
          link: { type: 'doc', id: 'cloud/set_up_gx_cloud' },
          items: [
            {
              type: 'link',
              label: 'Request a GX Cloud Beta account',
              href: '/docs/cloud/set_up_gx_cloud#request-a-gx-cloud-beta-account',
            },
            {
              type: 'link',
              label: 'Prepare your environment',
              href: '/docs/cloud/set_up_gx_cloud#prepare-your-environment',
            },
            {
              type: 'link',
              label: 'Get your user access token and organization ID',
              href: '/docs/cloud/set_up_gx_cloud#get-your-user-access-token-and-organization-id',
            },
            {
              type: 'link',
              label: 'Set the environment variables and start the GX Cloud agent',
              href: '/docs/cloud/set_up_gx_cloud#set-the-environment-variables-and-start-the-gx-cloud-agent',
            },
            {
              type: 'link',
              label: 'Secure your GX API Data Source connection strings',
              href: '/docs/cloud/set_up_gx_cloud#secure-your-gx-api-data-source-connection-strings',
            },
          ]
        },
        {
          type: 'category',
          label: 'Quickstarts',
          link: { type: 'doc', id: 'cloud/quickstarts/quickstart_lp' },
          items: [
            'cloud/quickstarts/snowflake_quickstart',
            'cloud/quickstarts/airflow_quickstart',
          ]
        },
        {
          type: 'category',
          label: 'Manage Data Assets',
          link: { type: 'doc', id: 'cloud/data_assets/manage_data_assets' },
          items: [
            {
              type: 'link',
              label: 'Create a Data Asset',
              href: '/docs/cloud/data_assets/manage_data_assets#create-a-data-asset',
            },
            {
              type: 'link',
              label: 'View Data Asset metrics',
              href: '/docs/cloud/data_assets/manage_data_assets#view-data-asset-metrics',
            },
            {
              type: 'link',
              label: 'Add an Expectation to a Data Asset column',
              href: '/docs/cloud/data_assets/manage_data_assets#add-an-expectation-to-a-data-asset-column',
            },
            {
              type: 'link',
              label: 'Add a Data Asset to an Existing Data Source',
              href: '/docs/cloud/data_assets/manage_data_assets#add-a-data-asset-to-an-existing-data-source',
            },
            {
              type: 'link',
              label: 'Edit a Data Asset',
              href: '/docs/cloud/data_assets/manage_data_assets#edit-a-data-asset',
            },
            {
              type: 'link',
              label: 'Delete a Data Asset',
              href: '/docs/cloud/data_assets/manage_data_assets#delete-a-data-asset',
            },
          ]
        },
        {
          type: 'category',
          label: 'Manage Expectations',
          link: { type: 'doc', id: 'cloud/expectations/manage_expectations' },
          items: [
            {
              type: 'link',
              label: 'Available Expectations',
              href: '/docs/cloud/expectations/manage_expectations#available-expectations',
            },
            {
              type: 'link',
              label: 'Add an Expectation',
              href: '/docs/cloud/expectations/manage_expectations#create-an-expectation',
            },
            {
              type: 'link',
              label: 'Edit an Expectation',
              href: '/docs/cloud/expectations/manage_expectations#edit-an-expectation',
            },
            {
              type: 'link',
              label: 'Delete an Expectation',
              href: '/docs/cloud/expectations/manage_expectations#delete-an-expectation',
            },
          ]
        },
        {
          type: 'category',
          label: 'Manage Expectation Suites',
          link: { type: 'doc', id: 'cloud/expectation_suites/manage_expectation_suites' },
          items: [
            {
              type: 'link',
              label: 'Automatically create an Expectation Suite that tests for missing data',
              href: '/docs/cloud/expectation_suites/manage_expectation_suites#automatically-create-an-expectation-suite-that-tests-for-missing-data',
            },
            {
              type: 'link',
              label: 'Create an empty Expectation Suite ',
              href: '/docs/cloud/expectation_suites/manage_expectation_suites#manually-create-an-empty-expectation-suite',
            },
            {
              type: 'link',
              label: 'Edit an Expectation Suite name',
              href: '/docs/cloud/expectation_suites/manage_expectation_suites#edit-an-expectation-suite-name',
            },
            {
              type: 'link',
              label: 'Delete an Expectation Suite',
              href: '/docs/cloud/expectation_suites/manage_expectation_suites#delete-an-expectation-suite',
            },
          ]
        },
        {
          type: 'category',
          label: 'Manage Validations',
          link: { type: 'doc', id: 'cloud/validations/manage_validations' },
          items: [
            {
              type: 'link',
              label: 'Run a Validation',
              href: '/docs/cloud/validations/manage_validations#run-a-validation',
            },
            {
              type: 'link',
              label: 'View Validation run history',
              href: '/docs/cloud/validations/manage_validations#view-validation-run-history',
            },
          ]
        },
        {
          type: 'category',
          label: 'Manage Checkpoints',
          link: { type: 'doc', id: 'cloud/checkpoints/manage_checkpoints' },
          items: [
            {
              type: 'link',
              label: 'Add a Checkpoint',
              href: '/docs/cloud/checkpoints/manage_checkpoints#add-a-checkpoint',
            },
            {
              type: 'link',
              label: 'Run a Checkpoint',
              href: '/docs/cloud/checkpoints/manage_checkpoints#run-a-checkpoint',
            },
            {
              type: 'link',
              label: 'Edit a Checkpoint name',
              href: '/docs/cloud/checkpoints/manage_checkpoints#edit-a-checkpoint-name',
            },
            {
              type: 'link',
              label: 'Edit a Checkpoint configuration',
              href: '/docs/cloud/checkpoints/manage_checkpoints#edit-a-checkpoint-configuration',
            },
            {
              type: 'link',
              label: 'Delete a Checkpoint',
              href: '/docs/cloud/checkpoints/manage_checkpoints#delete-a-checkpoint',
            },
          ]
        },
        {
          type: 'category',
          label: 'Manage users and access tokens',
          link: { type: 'doc', id: 'cloud/users/manage_users' },
          items: [
            {
              type: 'link',
              label: 'Invite a user',
              href: '/docs/cloud/users/manage_users#invite-a-user',
            },
            {
              type: 'link',
              label: 'Edit a user role',
              href: '/docs/cloud/users/manage_users#edit-a-user-role',
            },
            {
              type: 'link',
              label: 'Delete a user',
              href: '/docs/cloud/users/manage_users#delete-a-user',
            },
            {
              type: 'link',
              label: 'Create a user access token',
              href: '/docs/cloud/users/manage_users#create-a-user-access-token',
            },
            {
              type: 'link',
              label: 'Create an organization access token',
              href: '/docs/cloud/users/manage_users#create-an-organization-access-token',
            },
            {
              type: 'link',
              label: 'Delete a user or organization access token',
              href: '/docs/cloud/users/manage_users#delete-a-user-or-organization-access-token',
            },
          ]
        },
      ],
  gx_oss: [
        {type: 'doc', id: 'oss/intro', label: 'About GX'},
        {
          type: 'category',
          label: 'Get started with GX OSS',
          link: { type: 'doc', id: 'oss/guides/setup/get_started_lp' },
          items: [
            'oss/tutorials/quickstart',
            {
              type: 'doc', id: 'reference/learn/conceptual_guides/gx_overview', label: 'GX Overview'
            },
            'oss/get_started/get_started_with_gx_and_databricks',
            'oss/get_started/get_started_with_gx_and_sql',
          ]
        },
        {
          type: 'category',
          label: 'Configure your GX OSS environment',
          link: { type: 'doc', id: 'oss/guides/setup/setup_overview_lp' },
          items: [
            'oss/guides/setup/setup_overview',
            'oss/guides/setup/installation/install_gx',
            {
              type: 'category',
              label: 'Configure Data Contexts',
              link: { type: 'doc', id: 'oss/guides/setup/configure_data_contexts_lp' },
              items: [
                'oss/guides/setup/configuring_data_contexts/instantiating_data_contexts/instantiate_data_context',
                'oss/guides/setup/configuring_data_contexts/how_to_convert_an_ephemeral_data_context_to_a_filesystem_data_context',
                'oss/guides/setup/configuring_data_contexts/how_to_configure_credentials',
              ]
            },
            'oss/guides/setup/configuring_metadata_stores/configure_expectation_stores',
            'oss/guides/setup/configuring_metadata_stores/configure_result_stores',
            'oss/guides/setup/configuring_metadata_stores/how_to_configure_a_metricsstore',
            'oss/guides/setup/configuring_data_docs/host_and_share_data_docs',
          ]
        },
        {
          type: 'category',
          label: 'Connect to a Data Source',
          link: { type: 'doc', id: 'oss/guides/connecting_to_your_data/connect_to_data_lp' },
          items: [
            'oss/guides/connecting_to_your_data/fluent/filesystem/connect_filesystem_source_data',
            'oss/guides/connecting_to_your_data/fluent/in_memory/connect_in_memory_data',
            'oss/guides/connecting_to_your_data/fluent/database/connect_sql_source_data',
            {
              type: 'category',
              label: 'Manage Data Assets',
              link: { type: 'doc', id: 'oss/guides/connecting_to_your_data/manage_data_assets_lp' },
              items: [
                'oss/guides/connecting_to_your_data/fluent/batch_requests/how_to_request_data_from_a_data_asset',
                'oss/guides/connecting_to_your_data/fluent/data_assets/how_to_organize_batches_in_a_file_based_data_asset',
                'oss/guides/connecting_to_your_data/fluent/database/sql_data_assets',
              ]
            },
          ]
        },
        {
          type: 'category',
          label: 'Create Expectations',
          link: { type: 'doc', id: 'oss/guides/expectations/expectations_lp' },
          items: [
            'oss/guides/expectations/create_expectations_overview',
            {
              type: 'category',
              label: 'Manage Expectations and Expectation Suites',
              link: { type: 'doc', id: 'oss/guides/expectations/create_manage_expectations_lp' },
              items: [
                'oss/guides/expectations/how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly',
                'oss/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data',
                'oss/guides/expectations/how_to_edit_an_existing_expectationsuite',
                'oss/guides/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters',
                'oss/guides/expectations/advanced/how_to_dynamically_load_evaluation_parameters_from_a_database',
                'oss/guides/expectations/advanced/identify_failed_rows_expectations',
              ]
            },
            {
              type: 'category',
              label: 'Data Assistants',
              link: { type: 'doc', id: 'oss/guides/expectations/data_assistants_lp' },
              items: [
                'oss/guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant',
              ]
            },
            {
              type: 'category',
              label: 'Create Custom Expectations',
              link: { type: 'doc', id: 'oss/guides/expectations/custom_expectations_lp' },
              items: [
                'oss/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations',
                'oss/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations',
                'oss/guides/expectations/creating_custom_expectations/how_to_create_custom_batch_expectations',
                'oss/guides/expectations/creating_custom_expectations/how_to_create_custom_column_pair_map_expectations',
                'oss/guides/expectations/creating_custom_expectations/how_to_create_custom_multicolumn_map_expectations',
                'oss/guides/expectations/creating_custom_expectations/how_to_create_custom_regex_based_column_map_expectations',
                'oss/guides/expectations/creating_custom_expectations/how_to_create_custom_set_based_column_map_expectations',
                'oss/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations',
                'oss/guides/expectations/creating_custom_expectations/how_to_create_custom_parameterized_expectations',
                'oss/guides/expectations/creating_custom_expectations/how_to_add_support_for_the_auto_initializing_framework_to_a_custom_expectation',
              ]
            },
            {
              type: 'category',
              label: 'Add Features to Custom Expectations',
              link: { type: 'doc', id: 'oss/guides/expectations/add_features_custom_expectations_lp' },
              items: [
                'oss/guides/expectations/advanced/how_to_add_comments_to_expectations_and_display_them_in_data_docs',
                'oss/guides/expectations/features_custom_expectations/how_to_add_example_cases_for_an_expectation',
                'oss/guides/expectations/features_custom_expectations/how_to_add_input_validation_for_an_expectation',
                'oss/guides/expectations/features_custom_expectations/how_to_add_spark_support_for_an_expectation',
                'oss/guides/expectations/features_custom_expectations/how_to_add_sqlalchemy_support_for_an_expectation',
                'oss/guides/expectations/creating_custom_expectations/add_custom_parameters',
              ]
            },
            'oss/guides/expectations/creating_custom_expectations/how_to_use_custom_expectations',
          ]
        },
        {
          type: 'category',
          label: 'Validate Data',
          link: { type: 'doc', id: 'oss/guides/validation/validate_data_lp' },
          items: [
            'oss/guides/validation/validate_data_overview',
            {
              type: 'category',
              label: 'Manage Checkpoints',
              link: { type: 'doc', id: 'oss/guides/validation/checkpoints/checkpoint_lp' },
              items: [
                'oss/guides/validation/checkpoints/how_to_create_a_new_checkpoint',
                'oss/guides/validation/checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint',
                'oss/guides/validation/checkpoints/how_to_validate_multiple_batches_within_single_checkpoint',
                'oss/guides/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint',
                'oss/guides/validation/advanced/how_to_deploy_a_scheduled_checkpoint_with_cron',
              ]
            },
            {
              type: 'category',
              label: 'Configure Actions',
              link: { type: 'doc', id: 'oss/guides/validation/validation_actions/actions_lp' },
              items: [
                'oss/guides/validation/validation_actions/how_to_trigger_email_as_a_validation_action',
                'oss/guides/validation/validation_actions/how_to_collect_openlineage_metadata_using_a_validation_action',
                'oss/guides/validation/validation_actions/how_to_trigger_opsgenie_notifications_as_a_validation_action',
                'oss/guides/validation/validation_actions/how_to_trigger_slack_notifications_as_a_validation_action',
                'oss/guides/validation/advanced/how_to_get_data_docs_urls_for_custom_validation_actions',
              ]
            },
            'oss/guides/validation/limit_validation_results',
          ]
        },
        {
          type: 'category',
          label: 'Integrate',
          link: {
            type: 'generated-index',
            title: 'Integrate',
            description: 'Integrate GX OSS with commonly used data engineering tools.',
          },
          items: [
            {
              type: 'category',
              label: 'Amazon Web Services (AWS)',
              link: {
                type: 'doc',
                id: 'oss/deployment_patterns/aws_lp',
              },
              items: [
                'oss/deployment_patterns/how_to_use_great_expectations_in_aws_glue',
                'oss/deployment_patterns/how_to_instantiate_a_data_context_on_an_emr_spark_cluster',
                'oss/deployment_patterns/how_to_use_great_expectations_in_emr_serverless',
                'oss/deployment_patterns/how_to_use_gx_with_aws/how_to_use_gx_with_aws_using_cloud_storage_and_pandas',
                'oss/deployment_patterns/how_to_use_gx_with_aws/how_to_use_gx_with_aws_using_s3_and_spark',
                'oss/deployment_patterns/how_to_use_gx_with_aws/how_to_use_gx_with_aws_using_athena',
                'oss/deployment_patterns/how_to_use_gx_with_aws/how_to_use_gx_with_aws_using_redshift',
              ],
            },
            'oss/deployment_patterns/how_to_instantiate_a_data_context_hosted_environments',
            'oss/deployment_patterns/how_to_use_great_expectations_with_airflow',
            'oss/integrations/integration_datahub',
            'oss/deployment_patterns/how_to_use_great_expectations_in_deepnote',
            'oss/deployment_patterns/how_to_use_great_expectations_in_flyte',
            'oss/deployment_patterns/how_to_use_great_expectations_with_meltano',
            'oss/deployment_patterns/how_to_use_great_expectations_with_prefect',
            'oss/deployment_patterns/how_to_use_great_expectations_with_ydata_synthetic',
            'oss/integrations/integration_zenml',
          ]
        },
        { type: 'doc', id: 'oss/guides/miscellaneous/migration_guide' },
        { type: 'doc', id: 'oss/troubleshooting' },
        'oss/contributing/contributing',
        'oss/get_support',
      { type: 'doc', id: 'oss/changelog' },
      ],
  gx_apis: [
      {
      type: 'category',
      label: 'GX API',
      link: {
        type: 'generated-index',
        title: 'GX API',
        description: 'GX API reference content is generated from classes and methods docstrings.',
        slug: '/reference/api/'
      },
      items: [
        {
          type: 'autogenerated',
          dirName: 'reference/api'
        }
      ]
    },
  ],
  learn: [
      'reference/learn/conceptual_guides/expectation_classes',
      'reference/learn/conceptual_guides/metricproviders',
      'reference/learn/conceptual_guides/contributing_maturity',
      'reference/learn/usage_statistics',
      {
      type: 'category',
      label: 'Glossary',
      link: { type: 'doc', id: 'reference/learn/glossary' },
      items: [
        'reference/learn/terms/action',
        'reference/learn/terms/batch',
        'reference/learn/terms/batch_request',
        'reference/learn/terms/custom_expectation',
        'reference/learn/terms/checkpoint',
        'reference/learn/terms/datasource',
        'reference/learn/terms/data_context',
        'reference/learn/terms/data_asset',
        'reference/learn/terms/data_assistant',
        'reference/learn/terms/data_docs',
        'reference/learn/terms/evaluation_parameter',
        'reference/learn/terms/execution_engine',
        {
          type: 'category',
          label: 'Expectations',
          link: { type: 'doc', id: 'reference/learn/terms/expectation' },
          collapsed: true,
          items: [
            { type: 'doc', id: 'reference/learn/expectations/conditional_expectations' },
            { type: 'doc', id: 'reference/learn/expectations/distributional_expectations' },
            { type: 'doc', id: 'reference/learn/expectation_suite_operations' },
            { type: 'doc', id: 'reference/learn/expectations/result_format' },
            { type: 'doc', id: 'reference/learn/expectations/standard_arguments' }
          ]
        },
        'reference/learn/terms/expectation_suite',
        'reference/learn/terms/metric',
        'reference/learn/conceptual_guides/metricproviders',
        {
          type: 'category',
          label: 'Stores',
          link: { type: 'doc', id: 'reference/learn/terms/store' },
          items: [
            'reference/learn/terms/checkpoint_store',
            'reference/learn/terms/data_docs_store',
            'reference/learn/terms/evaluation_parameter_store',
            'reference/learn/terms/expectation_store',
            'reference/learn/terms/metric_store',
            'reference/learn/terms/validation_result_store'
          ]
        },
        'reference/learn/terms/renderer',
        'reference/learn/terms/supporting_resource',
        'reference/learn/terms/validator',
        'reference/learn/terms/validation_result'
      ]
    },
  ]
}

