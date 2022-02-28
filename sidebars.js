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
      ]
    },
    {
      type: 'category',
      label: 'Step 1: Setup',
      items: [
        { type: 'doc', id: 'guides/setup/setup_overview', label: 'Overview' },
        {
          type: 'category',
          label: 'How-to guides',
          items: [
            {
              type: 'category',
              label: 'Installation',
              items: [
                'guides/setup/installation/local',
                'guides/setup/installation/hosted_environment'
              ]
            },
            {
              type: 'category',
              label: 'Data Contexts',
              items: [
                'guides/setup/configuring_data_contexts/how_to_configure_a_new_data_context_with_the_cli',
                'guides/setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config',
                'guides/setup/configuring_data_contexts/how_to_configure_credentials',
                'guides/setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file'
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
            { type: 'doc', id: 'guides/setup/index', label: 'Setup: Index' }
          ]
        }
      ]
    },
    {
      type: 'category',
      label: 'Step 2: Connect to data',
      items: [
        { type: 'doc', id: 'guides/connecting_to_your_data/connect_to_data_overview', label: 'Overview' },
        {
          type: 'category',
          label: 'How-to guides',
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
            {
              type: 'category',
              label: 'Advanced',
              items: [
                'guides/connecting_to_your_data/advanced/how_to_configure_a_dataconnector_for_splitting_and_sampling_a_file_system_or_blob_store',
                'guides/connecting_to_your_data/advanced/how_to_configure_a_dataconnector_for_splitting_and_sampling_tables_in_sql'
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
          label: 'How-to guides',
          items: [
            {
              type: 'category',
              label: 'Core skills',
              items: [
                'guides/expectations/how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly',
                'guides/expectations/how_to_create_and_edit_expectations_in_bulk',
                'guides/expectations/how_to_create_and_edit_expectations_with_a_profiler',
                'guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data'
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
                'guides/expectations/creating_custom_expectations/overview',
                'guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations',
                'guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations',
                'guides/expectations/creating_custom_expectations/how_to_create_custom_parameterized_expectations',
                'guides/expectations/creating_custom_expectations/how_to_create_custom_metrics',
                {
                  type: 'category',
                  label: 'Adding Features to Custom Expectations',
                  items: [
                    'guides/expectations/features_custom_expectations/how_to_add_data_visualization_renderers_for_an_expectation',
                    'guides/expectations/features_custom_expectations/how_to_add_example_cases_for_an_expectation',
                    'guides/expectations/features_custom_expectations/how_to_add_input_validation_for_an_expectation',
                    'guides/expectations/features_custom_expectations/how_to_add_statement_renderers_for_an_expectation',
                    'guides/expectations/features_custom_expectations/how_to_add_spark_support_for_an_expectation',
                    'guides/expectations/features_custom_expectations/how_to_add_sqlalchemy_support_for_an_expectation'
                  ]
                }
              ]
            },
            {
              type: 'category',
              label: 'Advanced',
              items: [
                'guides/expectations/advanced/how_to_add_comments_to_expectations_and_display_them_in_data_docs',
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
      label: 'Step 4: Validate data',
      items: [
        { type: 'doc', id: 'guides/validation/validate_data_overview', label: 'Overview' },
        {
          type: 'category',
          label: 'How-to guides',
          items: []
        }
      ]
    },
    {
      type: 'category',
      label: 'Reference Architectures',
      items: []
    },
    {
      type: 'category',
      label: 'Contributing',
      items: [
        {
          type: 'category',
          label: 'Custom Expectations',
          items: [
            'guides/expectations/contributing/how_to_contribute_a_custom_expectation_to_great_expectations'
          ]
        }
      ]
    },
    {
      type: 'category',
      label: 'Reference',
      items: []
    },
    {
      type: 'category',
      label: 'Updates and migration',
      items: []
    }
  ]
}
