import React from 'react'
import Select from 'react-select'

class Article extends React.Component {
  shouldBeHidden () {
    return this.props.tags.some(tag => this.props.hiddenTags.includes(tag))
  }

  render () {
    return (
      <li style={{
        display: this.shouldBeHidden() ? 'none' : 'block'
      }}
      >
        <a href='docs/guides/connecting_to_your_data/database/postgres'>{this.props.title}</a> {this.props.tags.map((item, i) => (<em key={i} style={{ fontSize: '0.7em', display: 'inline', margin: '3px', padding: '0 3px', color: '#fff', background: '#ccc' }}>{item}</em>))}
      </li>
    )
  }
}

function removeItemsFromArray (items, array) {
  const itemsToRemove = new Set(items)
  return array.filter(item => !itemsToRemove.has(item))
}

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
      compute: null
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
    console.log('resetting state')
    this.setState({
      hiddenTags: [],
      installSelectedOption: null,
      metadataSelectedOption: null,
      dataDocsSelectedOption: null
    })
  }

  // <p>hiddenTags: {this.state.hiddenTags.map((tag) => (tag + ", "))}</p>
  // <p>installSelectedOption: {this.state.installSelectedOption}</p>
  // <p>metadataSelectedOption: {this.state.metadataSelectedOption}</p>
  // <p>dataDocsSelectedOption: {this.state.dataDocsSelectedOption}</p>
  render () {
    return (
      <div style={{ width: '1000px' }}>
        <h1>[ICON] Setup</h1>

        <ol>
          <li>Where will you install Great Expectations? <Select
            defaultValue={null}
                        // value={this.state.installSelectedOption}
            onChange={this.handleInstallChange}
            options={installOptions}
            isSearchable
                                                         />
          </li>
          <li>Where will you store your metadata? <Select
            defaultValue={null}
                        // value={this.state.metadataSelectedOption}
            onChange={this.handleMetadataStoreChange}
            options={metadataOptions}
            isSearchable
                                                  />
          </li>
          <li>Where will you store and host your Data Docs? <Select
            defaultValue={null}
                        // value={this.state.dataDocsSelectedOption}
            onChange={this.handleDataDocsChange}
            options={dataDocsOptions}
            isSearchable
                                                            />
          </li>
        </ol>
        <button onClick={() => this.reset()}>Reset Filters</button>

        <br />
        <br />
        <h2>Installation</h2>
        <ol>
          <Article title='How to install Great Expectations locally' tags={['install', 'install-locally']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to install Great Expectations in DataBricks' tags={['install', 'install-databricks']} hiddenTags={this.state.hiddenTags} />
        </ol>
        <h2>Configuration Type</h2>
        <ol>
          <Article title='How to configure a DataContext in yaml' tags={['configure', 'yaml']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to configure a DataContext in python code' tags={['configure', 'code', 'databricks']} hiddenTags={this.state.hiddenTags} />
        </ol>

        <h2>Configuring metadata stores</h2>
        <ol>
          <Article title='How to configure an Expectation store in Amazon S3' tags={['metadata-store', 'metadata-store-s3']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to configure an Expectation store in Azure blob storage' tags={['metadata-store', 'metadata-store-azure']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to configure an Expectation store in GCS' tags={['metadata-store', 'metadata-store-gcs']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to configure an Expectation store on a filesystem' tags={['metadata-store', 'metadata-store-filesystem']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to configure an Expectation store to PostgreSQL' tags={['metadata-store', 'metadata-store-database']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to configure a Validation Result store in Amazon S3' tags={['metadata-store', 'metadata-store-s3']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to configure a Validation Result store in Azure blob storage' tags={['metadata-store', 'metadata-store-azure']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to configure a Validation Result store in GCS' tags={['metadata-store', 'metadata-store-gcs']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to configure a Validation Result store on a filesystem' tags={['metadata-store', 'metadata-store-filesystem']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to configure a Validation Result store to PostgreSQL' tags={['metadata-store', 'metadata-store-database']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to configure a MetricsStore' tags={['metadata-store']} hiddenTags={this.state.hiddenTags} />
        </ol>

        <h2>Data Docs</h2>
        <ol>
          <Article title='How to add comments to Expectations and display them in Data Docs' tags={['datadocs']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to Create Renderers for Custom Expectations' tags={['datadocs']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to host and share Data Docs on a filesystem' tags={['datadocs', 'datadocs-filesystem']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to host and share Data Docs on Azure Blob Storage' tags={['datadocs', 'datadocs-azure']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to host and share Data Docs on GCS' tags={['datadocs', 'datadocs-gcs']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to host and share Data Docs on Amazon S3' tags={['datadocs', 'datadocs-s3']} hiddenTags={this.state.hiddenTags} />
        </ol>

        <h1>[ICON] Connecting to data</h1>
        <ol>
          <li>Where is your data?
            <Select
              defaultValue={null}
                            // value={this.state.installSelectedOption}
              onChange={this.handleDataLocationChange}
              options={dataLocationOptions}
              isSearchable
            />
          </li>
          <li>What will you use for compute?
            <Select
              defaultValue={null}
                            // value={this.state.metadataSelectedOption}
              onChange={this.handleComputeChange}
              options={computeOptions}
              isSearchable
            />
          </li>
        </ol>
        <button onClick={() => this.reset()}>Reset Filters</button>
        <h2>Configuring a Datasource</h2>
        <ol>
          <Article title='How to connect to your data on a filesystem using Pandas' tags={['configure-datasource', 'compute-pandas', 'data-location-filesystem']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to connect to your data on a filesystem using Spark' tags={['configure-datasource', 'compute-spark', 'data-location-filesystem']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to connect to your data on S3 using Pandas' tags={['configure-datasource', 'compute-pandas', 'data-location-s3']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to connect to your data on S3 using Spark' tags={['configure-datasource', 'compute-spark', 'data-location-s3']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to connect to your data on GCS using Pandas' tags={['configure-datasource', 'compute-pandas', 'data-location-gcs']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to connect to your data on GCS using Spark' tags={['configure-datasource', 'compute-spark', 'data-location-gcs']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to connect to your data on Azure using Pandas' tags={['configure-datasource', 'compute-pandas', 'data-location-azure']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to connect to your data on Azure using Spark' tags={['configure-datasource', 'compute-spark', 'data-location-azure']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to connect to your data in a Athena database' tags={['configure-datasource', 'compute-athena', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to connect to your data in a BigQuery database' tags={['configure-datasource', 'compute-bigquery', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to connect to your data in a MSSQL database' tags={['configure-datasource', 'compute-mssql', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to connect to your data in a MYSQL database' tags={['configure-datasource', 'compute-mysql', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to connect to your data in a Redshift database' tags={['configure-datasource', 'compute-redshift', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to connect to your data in a Snowflake database' tags={['configure-datasource', 'compute-snowflake', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to connect to your data in a Postgresql database' tags={['configure-datasource', 'compute-postgres', 'data-location-database']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to configure a self managed Spark Datasource' tags={['configure-datasource', 'compute-spark']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to configure an EMR Spark Datasource' tags={['configure-datasource', 'compute-spark', 'data-location-filesystem']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to configure a Databricks AWS Datasource' tags={['configure-datasource', 'compute-spark', 'data-location-s3']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to configure a Databricks Azure Datasource' tags={['configure-datasource', 'compute-spark', 'data-location-azure']} hiddenTags={this.state.hiddenTags} />

        </ol>
        <h2>Configuring a DataConnector</h2>
        <ol>
          <Article title='How to choose which DataConnector to use' tags={['data-connector']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to configure a ConfiguredAssetDataConnector' tags={['data-connector']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to configure an InferredAssetDataConnector' tags={['data-connector']} hiddenTags={this.state.hiddenTags} />
          <Article title='How to configure a Data Connector to Sort Batches' tags={['data-connector']} hiddenTags={this.state.hiddenTags} />
        </ol>
      </div>
    )
  }
};
