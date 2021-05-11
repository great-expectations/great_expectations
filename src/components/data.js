
// TODO taylor import this from the tested file
export const addDatasourceSnippet = `
# Import some libraries
from ruamel import yaml
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest

# Load a DataContext
context = ge.get_context()

# Save the Datasource configuration you created above into your Data Context
context.add_datasource(**yaml.load(datasource_yaml))
`

const pandasDatasourceConfig = `
datasource_yaml = f"""
name: taxi_datasource_with_runtime_data_connector
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
    default_runtime_data_connector_name:
        class_name: RuntimeDataConnector
        batch_identifiers:
            - default_identifier_name
"""
`

const sparkDatasourceConfig = `
datasource_yaml = f"""
name: taxi_datasource_with_runtime_data_connector
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: SparkExecutionEngine
data_connectors:
    default_runtime_data_connector_name:
        class_name: RuntimeDataConnector
        batch_identifiers:
            - default_identifier_name
"""
`

const postgresDatasourceConfig = `
config = """
name: my_postgres_datasource
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  connection_string: <YOUR_CONNECTION_STRING_HERE>
data_connectors:
   default_runtime_data_connector_name:
       class_name: RuntimeDataConnector
       batch_identifiers:
           - default_identifier_name
   default_inferred_data_connector_name:
       class_name: InferredAssetSqlDataConnector
       name: whole_table
"""
`

const pandasFilesystemBatchRequestSnippet = `
# PANDAS
batch_request = RuntimeBatchRequest(
    datasource_name="taxi_datasource_with_runtime_data_connector",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="default_name",  # this can be anything that identifies this data_asset for you
    runtime_parameters={"path": "<PATH TO YOUR DATA HERE>"},  # Add your path here.
    batch_identifiers={"default_identifier_name": "something_something"},
)
`
const pandasS3BatchRequestSnippet = `
# PANDAS
batch_request = RuntimeBatchRequest(
    datasource_name="taxi_datasource_with_runtime_data_connector",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="default_name",  # this can be anything that identifies this data_asset for you
    runtime_parameters={"path": "s3://<YOUR BUCKET>/<PATH TO YOUR DATA>"},  # Add your bucket and path here.
    batch_identifiers={"default_identifier_name": "something_something"},
)
`

const sparkFilesystemBatchRequestSnippet = `
# SPARK
batch_request = RuntimeBatchRequest(
    datasource_name="taxi_datasource_with_runtime_data_connector",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="default_name",  # this can be anything that identifies this data_asset for you
    runtime_parameters={"path": "<PATH TO YOUR DATA HERE>"},  # Add your path here.
    batch_identifiers={"default_identifier_name": "something_something"},
)
`

const postgresBatchRequestSnippet = `
# postgres
batch_request = ge.core.batch.RuntimeBatchRequest(
    datasource_name="my_postgres_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="default_name",  # this can be anything that identifies this data_asset for you
    runtime_parameters={"query": "SELECT * from taxi_data LIMIT 10"},
    batch_identifiers={"default_identifier_name": "something_something"},
)
`

export const data = {
  filesystem: {
    pandas: {
      compute: 'compute-pandas',
      location: 'data-location-filesystem',
      prerequisites: {
        dependencies: [],
        notes: [
          'Access to data on a local filesystem'
        ]
      },
      datasourceYaml: pandasDatasourceConfig,
      batchRequestSnippet: pandasFilesystemBatchRequestSnippet,
      congratsMessage: null,
      additionalNotes: null,
    },
    spark: {
      compute: 'compute-spark',
      location: 'data-location-filesystem',
      prerequisites: {
        dependencies: [],
        notes: [
          'Access to data on a local filesystem'
        ]
      },
      datasourceYaml: sparkDatasourceConfig,
      batchRequestSnippet: sparkFilesystemBatchRequestSnippet,
      congratsMessage: null,
      additionalNotes: null,
    }
  },
  s3: {
    pandas: {
      compute: 'compute-pandas',
      location: 'data-location-s3',
      prerequisites: {
        dependencies: [
          {
            name: 'boto3',
            description: 'AWS S3 python library used to access S3 buckets',
            pip: 'pip install boto3'
          }
        ],
        notes: [
          'Access to data on an AWS S3 bucket'
        ]
      },
      datasourceYaml: pandasDatasourceConfig,
      batchRequestSnippet: pandasS3BatchRequestSnippet,
      congratsMessage: null,
      additionalNotes: null,
    },
    spark: {
      compute: 'compute-spark',
      location: 'data-location-s3',
      prerequisites: {
        dependencies: [
          {
            name: 'boto3',
            description: 'AWS S3 python library used to access S3 buckets',
            pip: 'pip install boto3'
          }
        ],
        notes: [
          'Access to data on an AWS S3 bucket'
        ]
      },
      datasourceYaml: sparkDatasourceConfig,
      batchRequestSnippet: pandasS3BatchRequestSnippet,
      congratsMessage: null,
      additionalNotes: `
      Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vestibulum convallis sagittis ex at hendrerit. Etiam vel leo id lacus feugiat pretium sit amet quis nisl. Vestibulum at viverra leo, at vulputate est. Aliquam ex sapien, congue et blandit a, tincidunt non nisi. Quisque consectetur est a interdum consectetur. Etiam efficitur eros nibh, ut placerat ante congue eu. Maecenas fermentum ultrices eros, quis tempus augue tristique nec. Sed sit amet ultrices risus, et euismod turpis. Vivamus non diam nec erat blandit aliquet.
`,
    }
  },
  database: {
    postgres: {
      compute: 'compute-database',
      location: 'data-location-postgres',
      prerequisites: {
        dependencies: [
          {
            name: 'sqlalchemy',
            description: 'python library used to access databases',
            pip: 'pip install sqlalchemy'
          },
          {
            name: 'psycopg2',
            description: 'python library used to access postgres databases',
            pip: 'pip install psycopg2'
          }
        ],
        notes: [
          'Access to a postgres database'
        ]
      },
      datasourceYaml: postgresDatasourceConfig,
      datasourceAdditional: `Determine how to add credentials to configuration

 Great Expectations provides multiple methods of providing credentials for accessing databases. For our current example will use a , but other options include providing an Environment Variable, and loading from a Cloud Secret Store.

 For more information, please refer to [Additional Notes](#additional-notes).`,
      batchRequestSnippet: postgresBatchRequestSnippet,
      congratsMessage: null,
      additionalNotes: null,
    }
  }
}
