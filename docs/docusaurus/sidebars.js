module.exports = {
  gx_core: [
    {
      type: 'category',
      label: 'Introduction to Great Expectations',
      link: {type: 'doc', id: 'core/introduction/introduction'},
      items: [
        {
          type: 'doc',
          id: 'core/introduction/about_gx',
          label: 'About GX'
        },
        {
          type: 'doc',
          id: 'core/introduction/gx_overview',
          label: 'GX overview'
        },
        {
          type: 'doc',
          id: 'core/introduction/try_gx',
          label: 'Try GX'
        },
        {
          type: 'doc',
          id: 'core/introduction/community_resources',
          label: 'Community resources'
        },
      ],
    },
    {
      type: 'category',
      label: 'Set up a GX environment',
      link: {type: 'doc', id: 'core/set_up_a_gx_environment/set_up_a_gx_environment'},
      items: [
        {
          type: 'doc',
          id: 'core/set_up_a_gx_environment/install_python',
          label: 'Install Python'
        },
        {
          type: 'doc',
          id: 'core/set_up_a_gx_environment/install_gx',
          label: 'Install GX'
        },
        {
          type: 'doc',
          id: 'core/set_up_a_gx_environment/install_additional_dependencies',
          label: 'Install additional dependencies'
        },
        {
          type: 'doc',
          id: 'core/set_up_a_gx_environment/create_a_data_context',
          label: 'Create a Data Context'
        }
      ]
    },
    {
      type: 'category',
      label: 'Connect to data',
      link: {type: 'doc', id: 'core/connect_to_data/connect_to_data'},
      items: [
        {
          type: 'doc',
          id: 'core/connect_to_data/sql_data/sql_data',
          label: 'Connect to SQL data'
        },
        {
          type: 'doc',
          id: 'core/connect_to_data/filesystem_data/filesystem_data',
          label: 'Connect to Filesystem data'
        },
        {
          type: 'doc',
          id: 'core/connect_to_data/dataframes/dataframes',
          label: 'Connect to data in Dataframes'
        },
      ]
    },
    {
      type: 'category',
      label: 'Define Expectations',
      link: { type: 'doc', id: 'core/define_expectations/define_expectations' },
      items: [
        {
          type: 'doc',
          id: 'core/define_expectations/create_an_expectation',
          label: 'Create an Expectation'
        },
        {
          type: 'doc',
          id: 'core/define_expectations/retrieve_a_batch_of_test_data',
          label: 'Retrieve a Batch of sample data'
        },
        {
          type: 'doc',
          id: 'core/define_expectations/test_an_expectation',
          label: 'Test an Expectation'
        },
        {
          type: 'doc',
          id: 'core/define_expectations/organize_expectation_suites',
          label: 'Organize Expectations into an Expectation Suite'
        },
      ]
    },
    {
      type: 'category',
      label: 'Run Validations',
      link: {type: 'doc', id: 'core/run_validations/run_validations'},
      items: [
        {type: 'doc', id: 'core/run_validations/create_a_validation_definition'},
        {type: 'doc', id: 'core/run_validations/run_a_validation_definition'},
      ]
    },
    {
      type: 'category',
      label: 'Trigger actions based on results',
      link: {type: 'doc', id: 'core/trigger_actions_based_on_results/trigger_actions_based_on_results'},
      items: [
        {type: 'doc', id: 'core/trigger_actions_based_on_results/create_a_checkpoint_with_actions'},
        {type: 'doc', id: 'core/trigger_actions_based_on_results/choose_a_result_format/choose_a_result_format'},
        {type: 'doc', id: 'core/trigger_actions_based_on_results/run_a_checkpoint'},
      ]
    },
    {
      type: 'category',
      label: 'Customize Expectations',
      link: {type: 'doc', id: 'core/customize_expectations/customize_expectations'},
      items: [
        {type: 'doc', id: 'core/customize_expectations/expectation_row_conditions'},
        {type: 'doc', id: 'core/customize_expectations/define_a_custom_expectation_class'},
        {type: 'doc', id: 'core/customize_expectations/use_sql_to_define_a_custom_expectation'},
      ]
    },
    {
      type: 'doc',
      id: 'oss/changelog',
      label: 'Changelog'
    },
  ],
  gx_cloud: [
    {type: 'doc', id: 'cloud/why_gx_cloud'},
        {
          type: 'category',
          label: 'GX Cloud deployment patterns and architecture',
          link: { type: 'doc', id: 'cloud/about_gx' },
          items: [
            {
              type: 'link',
              label: 'GX Cloud architecture',
              href: '/docs/cloud/about_gx#gx-cloud-architecture',
            },
            {
              type: 'link',
              label: 'GX Agent',
              href: '/docs/cloud/about_gx#gx-agent',
            },
            {
              type: 'link',
              label: 'GX Cloud deployment patterns',
              href: '/docs/cloud/about_gx#gx-cloud-deployment-patterns',
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
              type: 'doc', 
              id: 'cloud/deploy_gx_agent' 
            },
          ]
        },
        {
          type: 'category',
          label: 'Connect GX Cloud',
          link: { type: 'doc', id: 'cloud/connect/connect_lp' },
          items: [
            'cloud/connect/connect_postgresql',
            'cloud/connect/connect_snowflake',
            'cloud/connect/connect_airflow',
            'cloud/connect/connect_python',
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
              label: 'Edit Data Source settings',
              href: '/docs/cloud/data_assets/manage_data_assets#edit-data-source-settings',
            },
            {
              type: 'link',
              label: 'Edit a Data Asset',
              href: '/docs/cloud/data_assets/manage_data_assets#edit-a-data-asset',
            },
            {
              type: 'link',
              label: 'Secure your GX API Data Source connection strings',
              href: '/docs/cloud/data_assets/manage_data_assets#secure-your-gx-api-data-source-connection-strings',
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
              href: '/docs/cloud/expectations/manage_expectations#add-an-expectation',
            },
            {
              type: 'link',
              label: 'Edit an Expectation',
              href: '/docs/cloud/expectations/manage_expectations#edit-an-expectation',
            },
            {
              type: 'link',
              label: 'View Expectation history',
              href: '/docs/cloud/expectations/manage_expectations#view-expectation-history',
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
              label: 'Create an Expectation Suite ',
              href: '/docs/cloud/expectation_suites/manage_expectation_suites#create-an-expectation-suite',
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
              label: 'Run a Validation on a Data Asset containing partitions',
              href: '/docs/cloud/validations/manage_validations#run-a-validation-on-a-data-asset-containing-partitions',
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
              "type": "link",
              "label": "Add a Validation to a Checkpoint",
              "href": "/docs/cloud/checkpoints/manage_checkpoints#add-a-validation-to-a-checkpoint"
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
              "type": "link",
              "label": "Configure the Checkpoint result format parameter",
          "href": "/docs/cloud/checkpoints/manage_checkpoints#configure-the-checkpoint-result-format-parameter"
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
          label: 'Manage alerts',
          link: { type: 'doc', id: 'cloud/alerts/manage_alerts' },
          items: [
            {
              type: 'link',
              label: 'Add a Slack alert',
              href: '/docs/cloud/alerts/manage_alerts#add-a-slack-alert',
            },
            {
              type: 'link',
              label: 'Edit a Slack alert',
              href: '/docs/cloud/alerts/manage_alerts#edit-a-slack-alert',
            },
            {
              type: 'link',
              label: 'Delete a Slack alert',
              href: '/docs/cloud/alerts/manage_alerts#delete-a-slack-alert',
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
  gx_apis: [
    {
      type: 'category',
      label: 'GX API',
      link: {
        type: 'doc',
        id: 'reference/index'
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
    {
      type: 'category',
      label: 'Data quality use cases',
      link: { type: 'doc', id: 'reference/learn/data_quality_use_cases/dq_use_cases_lp' },
      items: [
        'reference/learn/data_quality_use_cases/schema',
        'reference/learn/data_quality_use_cases/missingness',
        'reference/learn/data_quality_use_cases/distribution',
        'reference/learn/data_quality_use_cases/freshness',
        'reference/learn/data_quality_use_cases/volume'
      ]
    },
      'reference/learn/usage_statistics',
      'reference/learn/glossary'
  ],
}

