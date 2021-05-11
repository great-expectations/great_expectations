import React from 'react'
import Select from 'react-select'

import { installOptions, metadataOptions, dataDocsOptions, dataLocationOptions, computeOptions, databaseOptions } from '../components/options.js'
import { CodeSnippet } from '../components/snippet.js'
import { Prerequisites } from '../components/prerequisites.js'
import { Article } from '../components/article.js'
import { data, addDatasourceSnippet } from '../components/data.js'

class InteractiveHowtoGuide extends React.Component {
  getDatasourceConfigSnippet () {
    if (this.props.data) {
      return this.props.data.datasourceYaml
    }
    return '# Please answer the compute question'
  }

  getBatchRequestSnippet () {
    if (this.props.data) {
      return this.props.data.batchRequestSnippet
    }
    return '# Please answer the location question'
  }

  getDeps () {
    if (this.props.data) {
      return this.props.data.prerequisites.dependencies
    }
    return []
  }

  getNotes () {
    if (this.props.data) {
      return this.props.data.prerequisites.notes
    }
    return []
  }

  renderBlank () {
    return (
      <div>
        <h2>Please answer both questions for a customized how to guide.</h2>
      </div>
    )
  }

  renderAdditionalNotes () {
    if (this.props.data) {
      if (this.props.data.additionalNotes) {
        return (
          <div>
            <h2>Additional Notes</h2>
            {this.props.data.additionalNotes}
          </div>
        )
      }
    }
  }

  render () {
    if (this.props.location === null || this.props.compute === null) {
      return this.renderBlank()
    }
    return (
      <div>
        <Prerequisites deps={this.getDeps()} notes={this.getNotes()} />
        <h2>Steps</h2>

        <h3>1. Add the datasource to your project</h3>

        Using this example configuration:
        <CodeSnippet code={this.getDatasourceConfigSnippet()} />
        Add the datasource by using the CLI or python APIs:
        <CodeSnippet code={addDatasourceSnippet} />
        {this.props.datasourceAdditional}

        <h3>2. Write a `BatchRequest`</h3>

        In a `RuntimeBatchRequest` the `data_asset_name` can be any unique name to identify this batch of data.'
        Please update the `data_asset_name` to something meaningful to you.
        Add the path to your csv on a filesystem to the `path` key under `runtime_parameters` and run the following code:

        <CodeSnippet code={this.getBatchRequestSnippet()} />

        <h3>3. Test your datasource configuration by getting a Batch</h3>
        <CodeSnippet code='batch = context.get_batch(batch_request=batch_request)' />

        <p>Congratulations! If no errors are shown here, you've just connected to your data on {this.props.location} using {this.props.compute}</p>

        {this.renderAdditionalNotes()}
      </div>
    )
  }
}

function removeItemsFromArray (items, array) {
  const itemsToRemove = new Set(items)
  return array.filter(item => !itemsToRemove.has(item))
}

const buttonStyle = { fontSize: '1.5em', color: 'white', background: '#00bfa5', padding: '.5em', borderRadius: '8px' }

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
    // console.log('option selected', event.value)
    let hiddenTags = this.state.hiddenTags
    const tags = options.map((item) => item.value)
    // console.log('tags', tags)
    // remove all tags from hiddenTags
    hiddenTags = removeItemsFromArray(tags, hiddenTags)
    // add all options from hiddenTags
    hiddenTags = hiddenTags.concat(tags)
    // remove selected from hiddenTags
    hiddenTags = removeItemsFromArray([event.value], hiddenTags)
    // console.log('hiddenTags', hiddenTags)
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
    let location = this.state.dataLocation
    const dbValues = databaseOptions.map((db) => db.value)
    if (dbValues.includes(event.value)) {
      location = 'data-location-database'
    }
    const hiddenTags = this.handleChange(event, computeOptions)
    this.setState({ compute: event.value, dataLocation: location, hiddenTags: hiddenTags })
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

  getDataElement () {
    let result = null
    if (this.state.compute === 'compute-pandas') {
      if (this.state.dataLocation === 'data-location-s3') {
        result = data.s3.pandas
      } else if (this.state.dataLocation === 'data-location-filesystem') {
        result = data.filesystem.pandas
      }
    } else if (this.state.compute === 'compute-spark') {
      if (this.state.dataLocation === 'data-location-s3') {
        result = data.s3.spark
      } else if (this.state.dataLocation === 'data-location-filesystem') {
        result = data.filesystem.spark
      }
    } else if (this.state.compute === 'compute-postgres') {
      result = data.database.postgres
    } else {
      result = null
    };
    console.log('result = ', result)
    return result
  }

  getDatasourceAdditional () {
    const element = this.getDataElement()

    if (element) {
      return (
        <div>
          <h4>Additional stuff</h4>
          {element.datasourceAdditional}
        </div>
      )
    }
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
      <div>
        <div style={{ width: '500px', padding: '20px', float: 'left', background: '#eee' }}>
          <div style={{}}>
            <pre> [  M I N I M A P  ] </pre>
            <h1>Connecting to your data</h1>
            <p>Answering these two questions will customize this how to guide to your exact needs.</p>
            <ol>
              <li><strong>Where is your data?</strong>
                <Select
                  defaultValue={null}
                  onChange={this.handleDataLocationChange}
                  options={dataLocationOptions}
                  isSearchable
                />
              </li>
              <li><strong>What will you use for compute?</strong>
                <Select
                  defaultValue={null}
                  onChange={this.handleComputeChange}
                  options={computeOptions}
                  isSearchable
                />
              </li>
            </ol>
            <button style={buttonStyle} onClick={() => this.reset()}>Reset Answers</button>
          </div>

          <div style={{ marginTop: '3em' }}>
            <h2>Relevant How To Guides</h2>
            <h3>Configuring a Datasource</h3>
            <ol>
              <Article title='How to connect to your data on Pandas/filesystem' url='/docs/guides/connecting_to_your_data/filesystem/pandas' tags={['configure-datasource', 'compute-pandas', 'data-location-filesystem']} hiddenTags={this.state.hiddenTags} />
              <Article title='How to connect to your data on Pandas/S3' tags={['configure-datasource', 'compute-pandas', 'data-location-s3']} hiddenTags={this.state.hiddenTags} />
              <Article title='How to connect to your data on Spark/filesystem' url='/docs/guides/connecting_to_your_data/filesystem/spark' tags={['configure-datasource', 'compute-spark', 'data-location-filesystem']} hiddenTags={this.state.hiddenTags} />
              <Article title='How to connect to your data on self managed Spark' tags={['configure-datasource', 'compute-spark']} hiddenTags={this.state.hiddenTags} />
              <Article title='How to connect to your data on EMR Spark' tags={['configure-datasource', 'compute-spark', 'data-location-filesystem']} hiddenTags={this.state.hiddenTags} />
              <Article title='How to connect to your data on Databricks AWS' tags={['configure-datasource', 'compute-spark', 'data-location-s3']} hiddenTags={this.state.hiddenTags} />
              <Article title='How to connect to your data on Databricks Azure' tags={['configure-datasource', 'compute-spark', 'data-location-azure']} hiddenTags={this.state.hiddenTags} />
              <Article title='How to connect to your data on Athena' tags={['configure-datasource', 'compute-athena', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
              <Article title='How to connect to your data on BigQuery' tags={['configure-datasource', 'compute-bigquery', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
              <Article title='How to connect to your data on MSSQL' tags={['configure-datasource', 'compute-mssql', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
              <Article title='How to connect to your data on MySQL' tags={['configure-datasource', 'compute-mysql', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
              <Article title='How to connect to your data on Redshift' tags={['configure-datasource', 'compute-redshift', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
              <Article title='How to connect to your data on Snowflake' tags={['configure-datasource', 'compute-snowflake', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
              <Article title='How to connect to your data on Postgres' tags={['configure-datasource', 'compute-postgres', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
            </ol>
            <h3>Configuring a DataConnector</h3>
            <ol>
              <Article title='How to choose which DataConnector to use' tags={['data-connector']} hiddenTags={this.state.hiddenTags} />
              <Article title='How to configure a ConfiguredAssetDataConnector' tags={['data-connector']} hiddenTags={this.state.hiddenTags} />
              <Article title='How to configure an InferredAssetDataConnector' tags={['data-connector']} hiddenTags={this.state.hiddenTags} />
              <Article title='How to configure a Data Connector to Sort Batches' tags={['data-connector']} hiddenTags={this.state.hiddenTags} />
            </ol>
          </div>
        </div>

        <div style={{ padding: '20px', marginLeft: '500px', marginRight: '100px', minWidth: '1000px', background: 'white' }}>
          <h2>Your configuration guide</h2>
          <p>This guide will help you connect to your data stored in <strong>{this.state.dataLocation}</strong> using <strong>{this.state.compute}</strong>.
            This will allow you to work with your data in Great Expectations and <a href='#'>create expectation suites</a>, <a href='#'>validate your data</a> and more.
          </p>
          <div style={{ visibility: this.state.stepsViewerVisible ? 'visible' : 'hidden' }}>
            <InteractiveHowtoGuide
              data={this.getDataElement()}
              location={this.state.dataLocation}
              compute={this.state.compute}
              datasourceAdditional={this.getDatasourceAdditional()}
            />
          </div>
        </div>
      </div>
    )
    // <button style={buttonStyle} onClick={() => this.toggleStepsViewer()}>Toggle Steps</button>
  }
};
