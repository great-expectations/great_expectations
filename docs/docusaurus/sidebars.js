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
          id: 'core/introduction/community_resources',
          label: 'Community Resources'
        },
        {
          type: 'doc',
          id: 'core/introduction/try_gx',
          label: '🚧 Try GX'
        },
      ],
    },
    {
      type: 'category',
      label: 'Install and manage GX 1.0',
      link: {type: 'doc', id: 'core/installation_and_setup/installation_and_setup'},
      items: [
        {
          type: 'doc',
          id: 'core/installation_and_setup/install_gx',
          label: 'Install GX 1.0'
        },
        {
          type: 'doc',
          id: 'core/installation_and_setup/additional_dependencies/additional_dependencies',
          label: 'Install additional dependencies'
        },
        {
          type: 'category',
          label: 'Manage Data Contexts',
          link: {type: 'doc', id: 'core/installation_and_setup/manage_data_contexts'},
          items: [
            {
              type: 'link',
              label: 'Request a Data Context',
              href: '/docs/1.0-prerelease/core/installation_and_setup/manage_data_contexts#request-a-data-context',
            },
            {
              type: 'link',
              label: 'Initialize a new Data Context',
              href: '/docs/1.0-prerelease/core/installation_and_setup/manage_data_contexts#initialize-a-new-data-context',
            },
            {
              type: 'link',
              label: 'Connect to an existing Data Context',
              href: '/docs/1.0-prerelease/core/installation_and_setup/manage_data_contexts#connect-to-an-existing-data-context',
            },
            {
              type: 'link',
              label: 'Export an Ephemeral Data Context to a new File Data Context',
              href: '/docs/1.0-prerelease/core/installation_and_setup/manage_data_contexts#export-an-ephemeral-data-context-to-a-new-file-data-context',
            },
            {
              type: 'link',
              label: 'View a Data Context configuration',
              href: '/docs/1.0-prerelease/core/installation_and_setup/manage_data_contexts#view-a-data-context-configuration',
            },
          ]
        },
        {
          type: 'category',
          label: 'Manage credentials',
          link: {type: 'doc', id: 'core/installation_and_setup/manage_credentials'},
          items: [
            {
              type: 'link',
              label: 'Environment variables',
              href: '/docs/1.0-prerelease/core/installation_and_setup/manage_credentials#environment-variables',
            },
            {
              type: 'link',
              label: 'YAML file',
              href: '/docs/1.0-prerelease/core/installation_and_setup/manage_credentials#yaml-file',
            },
            {
              type: 'link',
              label: 'Secrets manager',
              href: '/docs/1.0-prerelease/core/installation_and_setup/manage_credentials#secrets-manager',
            },
          ]
        },
        {
          type: 'doc',
          id: 'core/installation_and_setup/manage_metadata_stores',
          label: '🚧 Manage Metadata Stores'
        },
        {
          type: 'doc',
          id: 'core/installation_and_setup/manage_data_docs',
          label: 'Manage Data Docs'
        },
      ]
    },
    {
      type: 'category',
      label: '🚧 Manage and access data',
      link: {type: 'doc', id: 'core/manage_and_access_data/manage_and_access_data'},
      items: [
        {
          type: 'category',
          label: 'Connect to and request data',
          link: {type: 'doc', id: 'core/manage_and_access_data/connect_to_data/connect_to_data'},
          items: [
            {
              type: 'doc',
              id: 'core/manage_and_access_data/connect_to_data/file_system/file_system',
              label: '🚧 Connect to file system data'
            },
            {
              type: 'doc',
              id: 'core/manage_and_access_data/connect_to_data/in_memory/in_memory',
              label: '🚧 Connect to in memory data'
            },
            {
              type: 'doc',
              id: 'core/manage_and_access_data/connect_to_data/sql/sql',
              label: '🚧 Connect to SQL database data'
            },
            {
              type: 'doc',
              id: 'core/manage_and_access_data/request_data',
              label: '🚧 Request data'
            },
          ]
        },
        {
          type: 'category',
          label: '🚧 Manage Data Sources',
          link: {type: 'doc', id: 'core/manage_and_access_data/manage_data_sources/manage_data_sources'},
          items: [
            {
              type: 'link',
              label: '🚧 List available Data Sources',
              href: '/docs/1.0-prerelease/core/manage_and_access_data/manage_data_sources#list-available-data-sources',
            },
            {
              type: 'link',
              label: '🚧 Get an existing Data Source',
              href: '/docs/1.0-prerelease/core/manage_and_access_data/manage_data_sources#get-an-existing-data-source',
            },
            {
              type: 'link',
              label: '🚧 Delete a Data Source',
              href: '/docs/1.0-prerelease/core/manage_and_access_data/manage_data_sources#delete-a-data-source',
            },
          ]
        },
        {
          type: 'category',
          label: '🚧 Manage Data Assets',
          link: {type: 'doc', id: 'core/manage_and_access_data/manage_data_assets'},
          items: [
            {
              type: 'link',
              label: '🚧 List all Data Assets in a Data Source',
              href: '/docs/1.0-prerelease/core/manage_and_access_data/manage_data_assets#list-all-data-assets-in-a-data-source',
            },
            {
              type: 'link',
              label: '🚧 Get an existing Data Asset',
              href: '/docs/1.0-prerelease/core/manage_and_access_data/manage_data_assets#get-an-existing-data-asset',
            },
            {
              type: 'link',
              label: '🚧 Add a Partitioner to a Data Asset',
              href: '/docs/1.0-prerelease/core/manage_and_access_data/manage_data_assets#add-a-partitioner-to-a-data-asset',
            },
            {
              type: 'link',
              label: '🚧 Delete a Data Asset',
              href: '/docs/1.0-prerelease/core/manage_and_access_data/manage_data_assets#delete-a-data-asset',
            },
          ]
        },
        {
          type: 'category',
          label: '🚧 Manage Batch Requests',
          link: {type: 'doc', id: 'core/manage_and_access_data/manage_batch_requests'},
          items: [
            {
              type: 'doc',
              id: 'core/manage_and_access_data/manage_batch_requests',
              label: '🚧 Retrieve all Batches from a Data Asset'
            },
            {
              type: 'doc',
              id: 'core/manage_and_access_data/manage_batch_requests',
              label: '🚧 Retrieve specific Batches from a Data Asset'
            },
            {
              type: 'doc',
              id: 'core/manage_and_access_data/manage_batch_requests',
              label: '🚧 Iterate retrieved Batches'
            },
          ]
        },
        {
          type: 'category',
          label: '🚧 Manage Batches',
          link: {type: 'doc', id: 'core/manage_and_access_data/manage_batches'},
          items: [
            {
              type: 'doc',
              id: 'core/manage_and_access_data/manage_batches',
              label: '🚧 View a sample of the data in a Batch'
            },
            {
              type: 'doc',
              id: 'core/manage_and_access_data/manage_batches',
              label: '🚧 Validate a Batch against an Expectation'
            },
            {
              type: 'doc',
              id: 'core/manage_and_access_data/manage_batches',
              label: '🚧 Validate a Batch against an Expectation Suite'
            },
            <!--TODO: Validation is being moved into the Batch object, are there other API changes? -->
            {
              type: 'doc',
              id: 'core/manage_and_access_data/manage_batches',
              label: '🚧 Get the Batch Request for a given Batch'
            },
          ]
        },
      ]
    },
    {
      type: 'category',
      label: 'Create Expectations',
      link: { type: 'doc', id: 'core/create_expectations/create_expectations' },
      items: [
        // 'oss/guides/expectations/create_expectations_overview',
        {
          type: 'category',
          label: 'Manage Expectations',
          link: { type: 'doc', id: 'core/create_expectations/expectations/manage_expectations' },
          items: [
            {
              type: 'link',
              label: 'Create an Expectation',
              href: '/docs/1.0-prerelease/core/create_expectations/expectations/manage_expectations#create-an-expectation',
            },
            {
              type: 'link',
              label: 'Test an Expectation',
              href: '/docs/1.0-prerelease/core/create_expectations/expectations/manage_expectations#test-an-expectation',
            },
            {
              type: 'link',
              label: 'Modify an Expectation',
              href: '/docs/1.0-prerelease/core/create_expectations/expectations/manage_expectations#modify-an-expectation',
            },
            {
              type: 'link',
              label: 'Customize an Expectation Class',
              href: '/docs/1.0-prerelease/core/create_expectations/expectations/manage_expectations#customize-an-expectation-class',
            },
          ]
        },
      {
          type: 'category',
          label: 'Manage Expectation Suites',
          link: { type: 'doc', id: 'core/create_expectations/expectation_suites/manage_expectation_suites' },
          items: [
            {
              type: 'link',
              label: 'Create an Expectation Suite',
              href: '/docs/1.0-prerelease/core/create_expectations/expectation_suites/manage_expectation_suites#create-an-expectation-suite',
            },
            {
              type: 'link',
              label: 'Get an existing Expectation Suite',
              href: '/docs/1.0-prerelease/core/create_expectations/expectation_suites/manage_expectation_suites#get-an-existing-expectation-suite',
            },
            {
              type: 'link',
              label: 'Rename an Expectation Suite',
              href: '/docs/1.0-prerelease/core/create_expectations/expectation_suites/manage_expectation_suites#rename-an-expectation-suite',
            },
            {
              type: 'link',
              label: 'Delete an Expectation Suite',
              href: '/docs/1.0-prerelease/core/create_expectations/expectation_suites/manage_expectation_suites#delete-an-expectation-suite',
            },
            {
              type: 'link',
              label: 'Add Expectations',
              href: '/docs/1.0-prerelease/core/create_expectations/expectation_suites/manage_expectation_suites#add-expectations-to-an-expectation-suite',
            },
            {
              type: 'link',
              label: 'Get an Expectation',
              href: '/docs/1.0-prerelease/core/create_expectations/expectation_suites/manage_expectation_suites#get-an-expectation-from-an-expectation-suite',
            },
            {
              type: 'link',
              label: 'Edit a single Expectation',
              href: '/docs/1.0-prerelease/core/create_expectations/expectation_suites/manage_expectation_suites#edit-a-single-expectation-in-an-expectation-suite',
            },
            {
              type: 'link',
              label: 'Edit multiple Expectations',
              href: '/docs/1.0-prerelease/core/create_expectations/expectation_suites/manage_expectation_suites#edit-multiple-expectations-in-an-expectation-suite',
            },
            {
              type: 'link',
              label: 'Delete an Expectation',
              href: '/docs/1.0-prerelease/core/create_expectations/expectation_suites/manage_expectation_suites#delete-an-expectation-from-an-expectation-suite',
            },
          ]
        },
        // {
        //   type: 'category',
        //   label: 'Data Assistants',
        //   link: { type: 'doc', id: 'oss/guides/expectations/data_assistants_lp' },
        //   items: [
        //     'oss/guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_missingness_data_assistant',
        //   ]
        // },
      ]
    },
    {
      type: 'category',
      label: 'Validate data',
      link: {type: 'doc', id: 'core/validate_data/validate_data'},
      items: [
        {
          type: 'category',
          label: 'Manage Validation Definitions',
          link: { type: 'doc', id: 'core/validate_data/validation_definitions/manage_validation_definitions' },
          items: [
            {
              type: 'link',
              label: 'Create a Validation Definition',
              href: '/docs/1.0-prerelease/core/validate_data/validation_definitions/manage_validation_definitions#create-a-validation-definition',
            },
            {
              type: 'link',
              label: 'List available Validation Definitions',
              href: '/docs/1.0-prerelease/core/validate_data/validation_definitions/manage_validation_definitions#list-available-validation-definitions',
            },
            {
              type: 'link',
              label: 'Get a Validation Definition by name',
              href: '/docs/1.0-prerelease/core/validate_data/validation_definitions/manage_validation_definitions#get-a-validation-definition-by-name',
            },
            {
              type: 'link',
              label: 'Get Validation Definitions by attributes',
              href: '/docs/1.0-prerelease/core/validate_data/validation_definitions/manage_validation_definitions#get-validation-definitions-by-attributes',
            },
            {
              type: 'link',
              label: 'Delete a Validation Definition',
              href: '/docs/1.0-prerelease/core/validate_data/validation_definitions/manage_validation_definitions#delete-a-validation-definition',
            },
            {
              type: 'link',
              label: 'Duplicate a Validation Definition',
              href: '/docs/1.0-prerelease/core/validate_data/validation_definitions/manage_validation_definitions#duplicate-a-validation-definition',
            },
            {
              type: 'link',
              label: 'Run a Validation Definition',
              href: '/docs/1.0-prerelease/core/validate_data/validation_definitions/manage_validation_definitions#run-a-validation-definition',
            },
          ]
        },
        {
          type: 'category',
          label: 'Manage Checkpoints',
          link: { type: 'doc', id: 'core/validate_data/checkpoints/manage_checkpoints' },
          items: [
            {
              type: 'link',
              label: 'Create a Checkpoint',
              href: '/docs/1.0-prerelease/core/validate_data/checkpoints/manage_checkpoints#create-a-checkpoint',
            },
            {
              type: 'link',
              label: 'List available Checkpoints',
              href: '/docs/1.0-prerelease/core/validate_data/checkpoints/manage_checkpoints#list-available-checkpoints',
            },
            {
              type: 'link',
              label: 'Get a Checkpoint by name',
              href: '/docs/1.0-prerelease/core/validate_data/checkpoints/manage_checkpoints#get-a-checkpoint-by-name',
            },
            {
              type: 'link',
              label: 'Get Checkpoints by attributes',
              href: '/docs/1.0-prerelease/core/validate_data/checkpoints/manage_checkpoints#get-checkpoints-by-attributes',
            },
            {
              type: 'link',
              label: 'Update a Checkpoint',
              href: '/docs/1.0-prerelease/core/validate_data/checkpoints/manage_checkpoints#update-a-checkpoint',
            },
            {
              type: 'link',
              label: 'Delete a Checkpoint',
              href: '/docs/1.0-prerelease/core/validate_data/checkpoints/manage_checkpoints#delete-a-checkpoint',
            },
            {
              type: 'link',
              label: '🚧 Run a Checkpoint',
              href: '/docs/1.0-prerelease/core/validate_data/checkpoints/manage_checkpoints#run-a-checkpoint',
            },
          ]
        },
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
          ]
        },
        { type: 'doc', id: 'cloud/deploy_gx_agent' },
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
              "label": "Add a Validation and an Expectation Suite to a Checkpoint",
              "href": "/docs/cloud/checkpoints/manage_checkpoints#add-a-validation-and-an-expectation-suite-to-a-checkpoint"
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
      'reference/learn/usage_statistics',
      'reference/learn/glossary'
  ],
}

