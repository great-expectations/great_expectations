import React from 'react'
import Select from 'react-select'

import Highlight, { defaultProps } from 'prism-react-renderer'

class Article extends React.Component {
  shouldBeHidden () {
    return this.props.tags.some(tag => this.props.hiddenTags.includes(tag))
  }

  render () {
    return (
      <li style={{ fontWeight: "bold", fontSize: "17px",
        display: this.shouldBeHidden() ? 'none' : 'block'
      }}
      >
        <a href='/docs/guides/connecting_to_your_data/filesystem/pandas'>{this.props.title}</a>
      </li>
    )
    // {this.props.tags.map((item, i) => (<em key={i} style={{ fontSize: '0.7em', display: 'inline', margin: '3px', padding: '0 3px', color: '#fff', background: '#ccc' }}>{item}</em>))}
  }
}

defaultProps.language = 'python'

const noSelectionMessage = 'Please make a selction to see code specific to your choices'

// TODO taylor import this from the tested file
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

const addDatasourceSnippet = `
# Import some libraries
from ruamel import yaml
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest

# Load a DataContext
context = ge.get_context()

# Save the Datasource configuration you created above into your Data Context
context.add_datasource(**yaml.load(datasource_yaml))
`

const pandasBatchRequestSnippet = `
# PANDAS
batch_request = RuntimeBatchRequest(
    datasource_name="taxi_datasource_with_runtime_data_connector",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="default_name",  # this can be anything that identifies this data_asset for you
    runtime_parameters={"path": "<PATH TO YOUR DATA HERE>"},  # Add your path here.
    batch_identifiers={"default_identifier_name": "something_something"},
)
`

const sparkBatchRequestSnippet = `
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

class CodeSnippet extends React.Component {
  render () {
    return (
      <Highlight {...defaultProps} code={this.props.code.trim()}>
        {({ className, style, tokens, getLineProps, getTokenProps }) => (
          <pre className={className} style={style}>
            {tokens.map((line, i) => (
              <div {...getLineProps({ line, key: i })} key={i}>
                {line.map((token, key) => (
                  <span {...getTokenProps({ token, key })} key={i} />
                ))}
              </div>
            ))}
          </pre>
        )}
      </Highlight>
    )
  }
}

class InteractiveViewer extends React.Component {
  getDatasourceConfigSnippet () {
    if (this.props.compute === 'compute-pandas') {
      return pandasDatasourceConfig
    } else if (this.props.compute === 'compute-spark') {
      return sparkDatasourceConfig
    } else if (this.props.compute === 'compute-postgres') {
      return postgresDatasourceConfig
    } else {
      return noSelectionMessage
    };
  }

  getBatchRequestSnippet () {
    if (this.props.compute === 'compute-pandas') {
      return pandasBatchRequestSnippet
    } else if (this.props.compute === 'compute-spark') {
      return sparkBatchRequestSnippet
    } else if (this.props.compute === 'compute-postgres') {
      return postgresBatchRequestSnippet
    } else {
      return noSelectionMessage
    };
  }

  render () {
    return (
      <div>
        <h2>Steps</h2>

        <h3>1. Add the datasource to your project</h3>

        Using this example configuration:
        <CodeSnippet code={this.getDatasourceConfigSnippet()} />
        Add the datasource by using the CLI or python APIs:
        <CodeSnippet code={addDatasourceSnippet} />

        <h3>2. Write a `BatchRequest`</h3>

        In a `RuntimeBatchRequest` the `data_asset_name` can be any unique name to identify this batch of data.'
        Please update the `data_asset_name` to something meaningful to you.
        Add the path to your csv on a filesystem to the `path` key under `runtime_parameters` and run the following code:

        <CodeSnippet code={this.getBatchRequestSnippet()} />

        <h3>3. Test your datasource configuration by getting a Batch</h3>
        <CodeSnippet code='batch = context.get_batch(batch_request=batch_request)' />
      </div>
    )
  }
}

function removeItemsFromArray (items, array) {
  const itemsToRemove = new Set(items)
  return array.filter(item => !itemsToRemove.has(item))
}

const buttonStyle = { fontSize: '1.5em', color: 'white', background: '#00bfa5', padding: '.5em' }

const installOptions = [
  { value: 'install-locally', label: 'locally' },
  { value: 'install-databricks', label: 'DataBricks' }
]
const metadataOptions = [
  { value: 'metadata-store-filesystem', label: 'filesystem' },
  { value: 'metadata-store-s3', label: 's3' },
  { value: 'metadata-store-azure', label: 'azure' },
  { value: 'metadata-store-gcs', label: 'gcs' },
  { value: 'metadata-store-database', label: 'database' }
]
const dataDocsOptions = [
  { value: 'datadocs-filesystem', label: 'filesystem' },
  { value: 'datadocs-s3', label: 's3' },
  { value: 'datadocs-azure', label: 'azure' },
  { value: 'datadocs-gcs', label: 'gcs' }
]
const dataLocationOptions = [
  { value: 'data-location-database', label: 'database' },
  { value: 'data-location-filesystem', label: 'filesystem' },
  { value: 'data-location-s3', label: 's3' },
  { value: 'data-location-azure', label: 'azure' },
  { value: 'data-location-gcs', label: 'gcs' }
]
const computeOptions = [
  { value: 'compute-pandas', label: 'pandas' },
  { value: 'compute-spark', label: 'spark' },
  { value: 'compute-postgres', label: 'postgres' },
  { value: 'compute-mysql', label: 'mysql' },
  { value: 'compute-mssql', label: 'mssql' },
  { value: 'compute-bigquery', label: 'bigquery' },
  { value: 'compute-redshift', label: 'redshift' },
  { value: 'compute-snowflake', label: 'snowflake' },
  { value: 'compute-athena', label: 'athena' }
]

export default class TOC extends React.Component {
  constructor (props) {
    super(props)
    this.state = {
      hiddenTags: [],
      installSelectedOption: null,
      metadataSelectedOption: null,
      dataDocsSelectedOption: null,
      dataLocation: null,
      compute: null,
      stepsViewerVisible: true
    }
    this.handleInstallChange = this.handleInstallChange.bind(this)
    this.handleMetadataStoreChange = this.handleMetadataStoreChange.bind(this)
    this.handleDataDocsChange = this.handleDataDocsChange.bind(this)
    this.handleDataLocationChange = this.handleDataLocationChange.bind(this)
    this.handleComputeChange = this.handleComputeChange.bind(this)
  }

  handleChange (event, options) {
    console.log('option selected', event.value)
    let hiddenTags = this.state.hiddenTags
    const tags = options.map((item) => item.value)
    console.log('tags', tags)
    // remove all tags from hiddenTags
    hiddenTags = removeItemsFromArray(tags, hiddenTags)
    // add all options from hiddenTags
    hiddenTags = hiddenTags.concat(tags)
    // remove selected from hiddenTags
    hiddenTags = removeItemsFromArray([event.value], hiddenTags)
    console.log('hiddenTags', hiddenTags)
    return hiddenTags
  }

  handleInstallChange (event) {
    const hiddenTags = this.handleChange(event, installOptions)
    this.setState({ installSelectedOption: event.value, hiddenTags: hiddenTags })
  }

  handleMetadataStoreChange (event) {
    const hiddenTags = this.handleChange(event, metadataOptions)
    this.setState({ metadataSelectedOption: event.value, hiddenTags: hiddenTags })
  }

  handleDataDocsChange (event) {
    const hiddenTags = this.handleChange(event, dataDocsOptions)
    this.setState({ dataDocsSelectedOption: event.value, hiddenTags: hiddenTags })
  }

  handleDataLocationChange (event) {
    const hiddenTags = this.handleChange(event, dataLocationOptions)
    this.setState({ dataLocation: event.value, hiddenTags: hiddenTags })
  }

  handleComputeChange (event) {
    const hiddenTags = this.handleChange(event, computeOptions)
    this.setState({ compute: event.value, hiddenTags: hiddenTags })
  }

  reset () {
    this.setState({
      hiddenTags: [],
      installSelectedOption: null,
      metadataSelectedOption: null,
      dataDocsSelectedOption: null,
      dataLocation: null,
      compute: null
    })
  }

  toggleStepsViewer () {
    const stepsViewerVisible = this.state.stepsViewerVisible
    this.setState({ stepsViewerVisible: !stepsViewerVisible })
  }

  // <p>hiddenTags: {this.state.hiddenTags.map((tag) => (tag + ", "))}</p>
  // <p>installSelectedOption: {this.state.installSelectedOption}</p>
  // <p>metadataSelectedOption: {this.state.metadataSelectedOption}</p>
  // <p>dataDocsSelectedOption: {this.state.dataDocsSelectedOption}</p>
  render () {
    return (
      // <h1>[ICON] Setup</h1>
      //
      // <ol>
      // <li>Where will you install Great Expectations? <Select
      // defaultValue={null}
      // // value={this.state.installSelectedOption}
      // onChange={this.handleInstallChange}
      // options={installOptions}
      // isSearchable
      // />
      // </li>
      // <li>Where will you store your metadata? <Select
      // defaultValue={null}
      // // value={this.state.metadataSelectedOption}
      // onChange={this.handleMetadataStoreChange}
      // options={metadataOptions}
      // isSearchable
      // />
      // </li>
      // <li>Where will you store and host your Data Docs? <Select
      // defaultValue={null}
      // // value={this.state.dataDocsSelectedOption}
      // onChange={this.handleDataDocsChange}
      // options={dataDocsOptions}
      // isSearchable
      // />
      // </li>
      // </ol>
      // <button onClick={() => this.reset()}>Reset Filters</button>
      //
      // <br />
      // <br />
      // <h2>Installation</h2>
      // <ol>
      // <Article title='How to install Great Expectations locally' tags={['install', 'install-locally']} hiddenTags={this.state.hiddenTags} />
      // <Article title='How to install Great Expectations in DataBricks' tags={['install', 'install-databricks']} hiddenTags={this.state.hiddenTags} />
      // </ol>
      // <h2>Configuration Type</h2>
      // <ol>
      // <Article title='How to configure a DataContext in yaml' tags={['configure', 'yaml']} hiddenTags={this.state.hiddenTags} />
      // <Article title='How to configure a DataContext in python code' tags={['configure', 'code', 'databricks']} hiddenTags={this.state.hiddenTags} />
      // </ol>
      //
      // <h2>Configuring metadata stores</h2>
      // <ol>
      // <Article title='How to configure an Expectation store in Amazon S3' tags={['metadata-store', 'metadata-store-s3']} hiddenTags={this.state.hiddenTags} />
      // <Article title='How to configure an Expectation store in Azure blob storage' tags={['metadata-store', 'metadata-store-azure']} hiddenTags={this.state.hiddenTags} />
      // <Article title='How to configure an Expectation store in GCS' tags={['metadata-store', 'metadata-store-gcs']} hiddenTags={this.state.hiddenTags} />
      // <Article title='How to configure an Expectation store on a filesystem' tags={['metadata-store', 'metadata-store-filesystem']} hiddenTags={this.state.hiddenTags} />
      // <Article title='How to configure an Expectation store to PostgreSQL' tags={['metadata-store', 'metadata-store-database']} hiddenTags={this.state.hiddenTags} />
      // <Article title='How to configure a Validation Result store in Amazon S3' tags={['metadata-store', 'metadata-store-s3']} hiddenTags={this.state.hiddenTags} />
      // <Article title='How to configure a Validation Result store in Azure blob storage' tags={['metadata-store', 'metadata-store-azure']} hiddenTags={this.state.hiddenTags} />
      // <Article title='How to configure a Validation Result store in GCS' tags={['metadata-store', 'metadata-store-gcs']} hiddenTags={this.state.hiddenTags} />
      // <Article title='How to configure a Validation Result store on a filesystem' tags={['metadata-store', 'metadata-store-filesystem']} hiddenTags={this.state.hiddenTags} />
      // <Article title='How to configure a Validation Result store to PostgreSQL' tags={['metadata-store', 'metadata-store-database']} hiddenTags={this.state.hiddenTags} />
      // <Article title='How to configure a MetricsStore' tags={['metadata-store']} hiddenTags={this.state.hiddenTags} />
      // </ol>
      //
      // <h2>Data Docs</h2>
      // <ol>
      // <Article title='How to add comments to Expectations and display them in Data Docs' tags={['datadocs']} hiddenTags={this.state.hiddenTags} />
      // <Article title='How to Create Renderers for Custom Expectations' tags={['datadocs']} hiddenTags={this.state.hiddenTags} />
      // <Article title='How to host and share Data Docs on a filesystem' tags={['datadocs', 'datadocs-filesystem']} hiddenTags={this.state.hiddenTags} />
      // <Article title='How to host and share Data Docs on Azure Blob Storage' tags={['datadocs', 'datadocs-azure']} hiddenTags={this.state.hiddenTags} />
      // <Article title='How to host and share Data Docs on GCS' tags={['datadocs', 'datadocs-gcs']} hiddenTags={this.state.hiddenTags} />
      // <Article title='How to host and share Data Docs on Amazon S3' tags={['datadocs', 'datadocs-s3']} hiddenTags={this.state.hiddenTags} />
      // </ol>
      <div style={{ margin: '3em' }}>
        <div style={{ width: '400px', margin: '20px', float: 'left' }}>
          <h1>Connecting to your data</h1>
          <p>Answering these two questions will customize this how to guide to your exact needs.</p>
          <ol>
            <li><strong>Where is your data?</strong>
              <Select
                defaultValue={null}
                              // value={this.state.installSelectedOption}
                onChange={this.handleDataLocationChange}
                options={dataLocationOptions}
                isSearchable
              />
            </li>
            <li><strong>What will you use for compute?</strong>
              <Select
                defaultValue={null}
                              // value={this.state.metadataSelectedOption}
                onChange={this.handleComputeChange}
                options={computeOptions}
                isSearchable
              />
            </li>
          </ol>
          <button style={buttonStyle} onClick={() => this.reset()}>Reset Answers</button>
        </div>

        <div style={{ padding: '20px', marginLeft: '500px', background: '#eee' }}>
          <h1>How To Guides</h1>
          <h2>Configuring a Datasource</h2>
          <ol>
            <Article title='How to configure a Pandas/filesystem Datasource' tags={['configure-datasource', 'compute-pandas', 'data-location-filesystem']} hiddenTags={this.state.hiddenTags} />
            <Article title='How to configure a Pandas/S3 Datasource' tags={['configure-datasource', 'compute-pandas', 'data-location-s3']} hiddenTags={this.state.hiddenTags} />
            <Article title='How to configure a Spark/filesystem Datasource' tags={['configure-datasource', 'compute-spark', 'data-location-filesystem']} hiddenTags={this.state.hiddenTags} />
            <Article title='How to configure a self managed Spark Datasource' tags={['configure-datasource', 'compute-spark']} hiddenTags={this.state.hiddenTags} />
            <Article title='How to configure an EMR Spark Datasource' tags={['configure-datasource', 'compute-spark', 'data-location-filesystem']} hiddenTags={this.state.hiddenTags} />
            <Article title='How to configure a Databricks AWS Datasource' tags={['configure-datasource', 'compute-spark', 'data-location-s3']} hiddenTags={this.state.hiddenTags} />
            <Article title='How to configure a Databricks Azure Datasource' tags={['configure-datasource', 'compute-spark', 'data-location-azure']} hiddenTags={this.state.hiddenTags} />
            <Article title='How to configure an Athena Datasource' tags={['configure-datasource', 'compute-athena', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
            <Article title='How to configure a BigQuery Datasource' tags={['configure-datasource', 'compute-bigquery', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
            <Article title='How to configure a MSSQL Datasource' tags={['configure-datasource', 'compute-mssql', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
            <Article title='How to configure a MySQL Datasource' tags={['configure-datasource', 'compute-mysql', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
            <Article title='How to configure a Redshift Datasource' tags={['configure-datasource', 'compute-redshift', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
            <Article title='How to configure a Snowflake Datasource' tags={['configure-datasource', 'compute-snowflake', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
            <Article title='How to configure a Postgres Datasource' tags={['configure-datasource', 'compute-postgres', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
          </ol>
          <h2>Configuring a DataConnector</h2>
          <ol>
            <Article title='How to choose which DataConnector to use' tags={['data-connector']} hiddenTags={this.state.hiddenTags} />
            <Article title='How to configure a ConfiguredAssetDataConnector' tags={['data-connector']} hiddenTags={this.state.hiddenTags} />
            <Article title='How to configure an InferredAssetDataConnector' tags={['data-connector']} hiddenTags={this.state.hiddenTags} />
            <Article title='How to configure a Data Connector to Sort Batches' tags={['data-connector']} hiddenTags={this.state.hiddenTags} />
          </ol>
        </div>

        <hr />
        <h2>Your selection:</h2>
        <p>
          Follow the steps below to connect to my data stored on/in <strong>{this.props.dataLocation}</strong> using <strong>{this.props.compute}</strong>.
          This will allow you to work with your data in Great Expectations and <a href='#'>create expectation suites</a>, <a href='#'>validate your data</a> and more.
        </p>
        <button style={buttonStyle} onClick={() => this.toggleStepsViewer()}>Toggle Steps</button>
        <div style={{ visibility: this.state.stepsViewerVisible ? 'visible' : 'hidden' }}>
          <InteractiveViewer location={this.state.dataLocation} compute={this.state.compute} code={pandasDatasourceConfig} />
        </div>

      </div>
    )
  }
};
