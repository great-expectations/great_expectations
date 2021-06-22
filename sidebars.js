module.exports = {
  docs: [
    {
      type: 'category',
      label: 'Introduction',
      collapsed: false,
      items: [
        { type: 'doc', id: 'intro' },
        { type: 'doc', id: 'why-use-ge' }
      ]
    },
    {
      type: 'category',
      label: 'Getting started with Great Expectations',
      items: [
        'tutorials/getting-started/intro',
        'tutorials/getting-started/initialize-a-data-context',
        'tutorials/getting-started/connect-to-data',
        'tutorials/getting-started/create-your-first-expectations',
        'tutorials/getting-started/check-out-data-docs',
        'tutorials/getting-started/validate-your-data',
        'tutorials/getting-started/customize-your-deployment'
      ]
    },
    {
    type: 'category',
    label: 'Deployment Patterns',
    items: [
        'deployment_patterns/how-to-instantiate-a-data-context-on-an-emr-spark-cluster',
        'deployment_patterns/how-to-instantiate-a-data-context-on-databricks-spark-cluster',
        'deployment_patterns/how-to-run-a-checkpoint-in-airflow',
        {
          type: 'category',
          label: 'Contributing',
          items:[
            'deployment_patterns/contributing/how-to-add-a-new-deployment-pattern-document',
            'deployment_patterns/contributing/how-to-contribute-to-an-existing-deployment-pattern-document'
          ]
        },
      ]
    },
    {
      type: 'category',
      label: 'How to Guides',
      items: [
        {
          type: 'category',
          label: '⚙️ Setting up Great Expectations',
          items: [
            'guides/setup/how-to-instantiate-a-data-context',

            {
              type: 'category',
              label: 'Installation',
              items: [
                'guides/setup/installation/local',
                'guides/setup/installation/databricks',
                'guides/setup/installation/spark-emr'
              ]
            },
            {
              type: 'category',
              label: 'Configuring Data Contexts',
              items: [
                'guides/setup/configuring-data-contexts/how-to-create-a-new-data-context-with-the-cli',
                'guides/setup/configuring-data-contexts/how-to-configure-datacontext-components-using-test_yaml_config',
                'guides/setup/configuring-data-contexts/how-to-configure-credentials-using-a-yaml-file-or-environment-variables',
                'guides/setup/configuring-data-contexts/how-to-configure-credentials-using-a-secrets-store',
                'guides/setup/configuring-data-contexts/how-to-instantiate-a-data-context-without-a-yml-file',
              ]
            },
            {
              type: 'category',
              label: 'Configuring metadata Stores',
              items: [
                'guides/setup/configuring-metadata-stores/how-to-configure-an-expectation-store-in-amazon-s3',
                'guides/setup/configuring-metadata-stores/how-to-configure-an-expectation-store-in-azure-blob-storage',
                'guides/setup/configuring-metadata-stores/how-to-configure-an-expectation-store-in-gcs',
                'guides/setup/configuring-metadata-stores/how-to-configure-an-expectation-store-on-a-filesystem',
                'guides/setup/configuring-metadata-stores/how-to-configure-an-expectation-store-to-postgresql',
                'guides/setup/configuring-metadata-stores/how-to-configure-a-validation-result-store-in-amazon-s3',
                'guides/setup/configuring-metadata-stores/how-to-configure-a-validation-result-store-in-azure-blob-storage',
                'guides/setup/configuring-metadata-stores/how-to-configure-a-validation-result-store-in-gcs',
                'guides/setup/configuring-metadata-stores/how-to-configure-a-validation-result-store-on-a-filesystem',
                'guides/setup/configuring-metadata-stores/how-to-configure-a-validation-result-store-to-postgresql',
                'guides/setup/configuring-metadata-stores/how-to-configure-a-metricsstore'
              ]
            },
            {
              type: 'category',
              label: 'Configuring Data Docs',
              items: [
                'guides/setup/configuring-data-docs/how-to-host-and-share-data-docs-on-a-filesystem',
                'guides/setup/configuring-data-docs/how-to-host-and-share-data-docs-on-azure-blob-storage',
                'guides/setup/configuring-data-docs/how-to-host-and-share-data-docs-on-gcs',
                'guides/setup/configuring-data-docs/how-to-host-and-share-data-docs-on-amazon-s3'
              ]
            },
          ]
        },
        {
          type: 'category',
          label: '🔌 Connecting to your data',
          items: [
            'guides/connecting_to_your_data/how-to-configure-a-dataconnector-to-introspect-and-partition-a-file-system-or-blob-store',
            'guides/connecting_to_your_data/how-to-configure-a-dataconnector-to-introspect-and-partition-tables-in-sql',
            'guides/connecting_to_your_data/how-to-create-a-batch-of-data-from-an-in-memory-spark-or-pandas-dataframe',
            'guides/connecting_to_your_data/how-to-create-a-new-expectation-suite-using-the-cli',
            'guides/connecting_to_your_data/how-to-get-a-batch-of-data-from-a-configured-datasource',
            {
              type: 'category',
              label: '🚀 Database',
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
              label: '📁 Filesystem',
              items: [
                { type: 'doc', id: 'guides/connecting_to_your_data/filesystem/pandas' },
                { type: 'doc', id: 'guides/connecting_to_your_data/filesystem/spark' }
              ]
            },
            {
              type: 'category',
              label: '☁️ Cloud',
              items: [
                { type: 'doc', id: 'guides/connecting_to_your_data/cloud/s3/pandas' },
                { type: 'doc', id: 'guides/connecting_to_your_data/cloud/s3/spark' },
                { type: 'doc', id: 'guides/connecting_to_your_data/cloud/gcs/pandas' },
                { type: 'doc', id: 'guides/connecting_to_your_data/cloud/gcs/spark' },
                { type: 'doc', id: 'guides/connecting_to_your_data/cloud/azure/pandas' },
                { type: 'doc', id: 'guides/connecting_to_your_data/cloud/azure/spark' }
              ]
            },
            {
              type: 'category',
              label: '🔬 Advanced',
              items: [
                'guides/connecting_to_your_data/advanced/database_credentials',
                'guides/connecting_to_your_data/advanced/how-to-create-a-batch-from-a-sql-query',
                'guides/connecting_to_your_data/advanced/how-to-create-a-lightweight-data-catalog-by-applying-a-descriptive-profiler-to-a-configured-datasource',
                'guides/connecting_to_your_data/advanced/how-to-explore-changes-in-data-over-time-using-a-configured-datasource'
              ]
            }
          ]
        },
        {
          type: 'category',
          label: '🧪 Creating and editing Expectations for your data',
          items: [
            'guides/expectations/how-to-create-and-edit-expectations-based-on-domain-knowledge-without-inspecting-data-directly',
            'guides/expectations/how-to-create-and-edit-expectations-in-bulk',
            'guides/expectations/how-to-create-and-edit-expectations-with-a-profiler',
            'guides/expectations/how-to-create-and-edit-expectations-with-instant-feedback from-a-sample-batch-of-data',
            {
              type: 'category',
              label: '🔬 Advanced',
              items: [
                'guides/expectations/advanced/how-to-add-comments-to-expectations-and-display-them-in-data-docs',
                'guides/expectations/advanced/how-to-create-renderers-for-custom-expectations',
                'guides/expectations/advanced/how-to-create-a-new-expectation-suite-by-profiling-from-a-jsonschema-file',
                'guides/expectations/advanced/how-to-create-expectations-that-span-multiple-batches-using-evaluation-parameters',
                'guides/expectations/advanced/how-to-dynamically-load-evaluation-parameters-from-a-database'
              ]
            },
            {
              type: 'category',
              label: 'Configuring Profilers',
              items: []
            },
            {
              type: 'category',
              label: 'Contributing',
              items: [
                'guides/expectations/contributing/how-to-contribute-a-new-expectation-to-great-expectations'
              ]
            },
            {
              type: 'category',
              label: 'Creating Custom Expectations',
              items: [
                'guides/expectations/creating_custom_expectations/how-to-create-custom-expectations',
                'guides/expectations/creating_custom_expectations/how-to-create-custom-expectations-from-a-sql-query',
                'guides/expectations/creating_custom_expectations/how-to-create-custom-parameterized-expectations',
              ]
            },
          ]
        },
        {
          type: 'category',
          label: '✅ Validating your data',
          items: [
            'guides/validation/how-to-validate-data-by-running-a-checkpoint',
            {
              type: 'category',
              label: 'Advanced',
              items: [
                'guides/validation/advanced/how-to-deploy-a-scheduled-checkpoint-with-cron',
                'guides/validation/advanced/how-to-implement-custom-notifications',
                'guides/validation/advanced/how-to-validate-data-without-a-checkpoint'
              ]
            },
            {
              type: 'category',
              label: 'Checkpoints',
              items: [
                'guides/validation/checkpoints/how-to-add-validations-data-or-suites-to-a-checkpoint',
                'guides/validation/checkpoints/how-to-create-a-new-checkpoint',
                'guides/validation/checkpoints/how-to-configure-a-new-checkpoint-using-test_yaml_config'
              ]
            },
            {
              type: 'category',
              label: 'Contributing',
              items: [
                'guides/validation/contributing/how-to-contribute-a-new-validation-action',
              ]
            },
            {
              type: 'category',
              label: 'Validation Actions',
              items: [
                'guides/validation/validation_actions/how-to-store-validation-results-as-a-validation-action',
                'guides/validation/validation_actions/how-to-trigger-email-as-a-validation-action',
                'guides/validation/validation_actions/how-to-trigger-opsgenie-notifications-as-a-validation-action',
                'guides/validation/validation_actions/how-to-trigger-slack-notifications-as-a-validation-action',
                'guides/validation/validation_actions/how-to-update-data-docs-as-a-validation-action',
              ]
            },
          ]
        },
        {
          type: 'category',
          label: '🧰 Miscellaneous',
          items: [
            { type: 'doc', id: 'guides/miscellaneous/how-to-write-a-how-to-guide'},
            { type: 'doc', id: 'guides/miscellaneous/how-to-use-the-project-check-config-command'},
            { type: 'doc', id: 'guides/miscellaneous/how-to-use-the-great-expectations-cli'},
            { type: 'doc', id: 'guides/miscellaneous/how-to-use-the-great-expectation-docker-images'},
            { type: 'doc', id: 'guides/miscellaneous/how-to-quickly-explore-expectations-in-a-notebook'},
            { type: 'doc', id: 'guides/miscellaneous/how-to-configure-notebooks-generated-by-suite-edit'},
            { type: 'doc', id: 'guides/miscellaneous/TEMPLATE:How-to-{stub}'},
            { type: 'doc', id: 'guides/miscellaneous/TEMPLATE:How-to-{do something}'},
            { type: 'doc', id: 'guides/miscellaneous/TEMPLATE:How-to-connect-to {some kind of data}'},
          ]
        },
      ]
    },
    {
      type: 'category',
      label: 'Reference',
      collapsed: true,
      items: [
            { type: 'doc', id: 'reference/core-concepts' }
          ]
    },
    {
      type: 'category',
      label: 'Community Resources',
      collapsed: true,
      items: [
            { type: 'doc', id: 'community' }
      ]
    },
    {
      type: 'category',
      label: 'Contributing',
      collapsed: true,
      items: [
        {
          type: 'doc', id: 'contributing/contributing'
        }
      ]
    },
    {
      type: 'category',
      label: 'Changelog',
      collapsed: true,
      items: [
        {
          type: 'doc', id: 'changelog'
        }
      ]
    }
  ]

}
