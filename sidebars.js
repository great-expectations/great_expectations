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
      label: 'How to Guides',
      items: [
        {
          type: 'category',
          label: '⚙️ Setting up Great Expectations',
          items: [
            'guides/setup/how-to-instantiate-a-data-context',

            {
              type: 'category',
              label: '🧰 Installation',
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
                'guides/setup/configuring-data-contexts/how-to-instantiate-a-data-context-without-a-yml-file'
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
            }
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
              label: '💭 In-memory',
              items: [
                'guides/connecting_to_your_data/in_memory/pandas',
                'guides/connecting_to_your_data/in_memory/spark'
              ]
            },
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
                'guides/connecting_to_your_data/filesystem/pandas',
                'guides/connecting_to_your_data/filesystem/spark'
              ]
            },
            {
              type: 'category',
              label: '☁️ Cloud',
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
              label: 'Contributing',
              items: [
                'guides/connecting_to_your_data/contributing/how-to-add-support-for-a-new-sqlalchemy-dialect'
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
            'guides/expectations/how-to-create-and-edit-expectations-with-instant-feedback-from-a-sample-batch-of-data',
            {
              type: 'category',
              label: '🔬 Advanced',
              items: [
                'guides/expectations/advanced/how-to-add-comments-to-expectations-and-display-them-in-data-docs',
                'guides/expectations/advanced/how-to-create-renderers-for-custom-expectations',
                'guides/expectations/advanced/how-to-create-a-new-expectation-suite-by-profiling-from-a-jsonschema-file',
                'guides/expectations/advanced/how-to-create-expectations-that-span-multiple-batches-using-evaluation-parameters',
                'guides/expectations/advanced/how-to-dynamically-load-evaluation-parameters-from-a-database',
                'guides/expectations/advanced/how-to-create-a-new-expectation-suite-using-rule-based-profilers'
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
                'guides/expectations/creating_custom_expectations/how-to-create-custom-parameterized-expectations'
              ]
            }
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
                'guides/validation/contributing/how-to-contribute-a-new-validation-action'
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
                'guides/validation/validation_actions/how-to-update-data-docs-as-a-validation-action'
              ]
            },
            'guides/validation/how-to-validate-data-without-a-checkpoint'
          ]
        },
        {
          type: 'category',
          label: '🧰 Miscellaneous',
          items: [
            { type: 'doc', id: 'guides/miscellaneous/how-to-use-the-project-check-config-command' },
            { type: 'doc', id: 'guides/miscellaneous/how-to-use-the-great-expectations-cli' },
            { type: 'doc', id: 'guides/miscellaneous/how-to-quickly-explore-expectations-in-a-notebook' },
            { type: 'doc', id: 'guides/miscellaneous/how-to-configure-notebooks-generated-by-suite-edit' },
            { type: 'doc', id: 'guides/miscellaneous/how-to-add-comments-to-a-page-on-docs.greatexpectations.io' },
            { type: 'doc', id: 'guides/miscellaneous/how-to-use-the-great-expectation-docker-images' },
            { type: 'doc', id: 'guides/miscellaneous/how-to-write-a-how-to-guide' },
            { type: 'doc', id: 'guides/miscellaneous/how-to-template' }
          ]
        }
      ]
    },
    {
      type: 'category',
      label: 'Deployment Patterns',
      items: [
        'deployment_patterns/how-to-instantiate-a-data-context-hosted-environments',
        'deployment_patterns/how-to-instantiate-a-data-context-on-an-emr-spark-cluster',
        'deployment_patterns/how-to-instantiate-a-data-context-on-databricks-spark-cluster',
        'deployment_patterns/how-to-run-a-checkpoint-in-airflow',
        {
          type: 'category',
          label: 'Contributing',
          items: [
            'deployment_patterns/contributing/how-to-add-a-new-deployment-pattern-document',
            'deployment_patterns/contributing/how-to-contribute-to-an-existing-deployment-pattern-document'
          ]
        }
      ]
    },
    {
      type: 'category',
      label: 'Reference',
      items: [
        {
          type: 'category',
          label: 'Glossary of Expectations',
          items: [
            { type: 'doc', id: 'reference/glossary-of-expectations' }
          ]
        },
        {
          type: 'category',
          label: 'Core Concepts',
          items: [
            { type: 'doc', id: 'reference/core-concepts' },
            { type: 'doc', id: 'reference/checkpoints-and-actions' },
            { type: 'doc', id: 'reference/data-context' },
            { type: 'doc', id: 'reference/data-discovery' },
            { type: 'doc', id: 'reference/data-docs' },
            { type: 'doc', id: 'reference/datasources' },
            { type: 'doc', id: 'reference/evaluation-parameters' },
            { type: 'doc', id: 'reference/execution-engine' },
            {
              type: 'category',
              label: 'Expectations',
              collapsed: true,
              items: [
                { type: 'doc', id: 'reference/expectations/conditional-expectations' },
                { type: 'doc', id: 'reference/expectations/distributional-expectations' },
                { type: 'doc', id: 'reference/expectations/expectations' },
                { type: 'doc', id: 'reference/expectations/implemented-expectations' },
                { type: 'doc', id: 'reference/expectation-suite-operations' }
              ]
            },
            { type: 'doc', id: 'reference/metrics' },
            { type: 'doc', id: 'reference/profilers' },
            { type: 'doc', id: 'reference/expectations/result-format' },
            { type: 'doc', id: 'reference/expectations/standard-arguments' },
            { type: 'doc', id: 'reference/stores' },
            { type: 'doc', id: 'reference/dividing-data-assets-into-batches' },
            { type: 'doc', id: 'reference/validation' }
          ]
        },
        {
          type: 'category',
          label: 'Supporting Resources',
          items: [
            { type: 'doc', id: 'reference/supporting-resources' }
          ]
        },
        {
          type: 'category',
          label: 'Spare Parts',
          collapsed: true,
          items: [
            { type: 'doc', id: 'reference/spare-parts' }
          ]
        },
        {
          type: 'category',
          label: 'API Reference',
          collapsed: true,
          items: [
            { type: 'doc', id: 'reference/api-reference' }
          ]
        }
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
        { type: 'doc', id: 'contributing/contributing' },
        { type: 'doc', id: 'contributing/contributing-setup' },
        { type: 'doc', id: 'contributing/contributing-checklist' },
        { type: 'doc', id: 'contributing/contributing-github' },
        { type: 'doc', id: 'contributing/contributing-test' },
        { type: 'doc', id: 'contributing/contributing-maturity' },
        { type: 'doc', id: 'contributing/contributing-style' },
        { type: 'doc', id: 'contributing/contributing-misc' }
      ]
    },
    {
      type: 'category',
      label: 'Changelog',
      collapsed: true,
      items: [
        { type: 'doc', id: 'changelog' }
      ]
    }
  ]
}
