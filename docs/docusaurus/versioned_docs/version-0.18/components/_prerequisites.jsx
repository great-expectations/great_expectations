import React from 'react'
import GxData from './_data.jsx'
import VersionedLink from '@site/src/components/VersionedLink'

/**
 * A flexible Prerequisites admonition block.
 * Styling/structure was updated on 12/28/2022 to include theme
 * Default entries configurable by passing in props added on 12/29/2022
 *
 * Usage with only defaults
 *
 * <Prerequisites>
 *
 * Usage with additional list items
 *
 * <Prerequisites>
 *
 * - Have access to data on a filesystem
 *
 * </Prerequisites>
 *
 * Usage with default values from props:
 *
 * <Prerequisites requireInstallation={true}>
 *
 * </Prerequisites>
 *
 * Available default entries from props:
 *   requirePython: Valid values are {true} or {false}
 *   requireInstallation: Valid values are {true} or {false}
 *   requireDataContext: Valid values are {true} or {false}
 *   requireSourceData: Valid values are 'filesystem' or 'SQL'
 *   requireDatasource: Valid values are 'Pandas', 'Spark', 'SQL', {true}, or {false},
 *     requireSourceData should not be needed if requireDatasource has been provided.
 *     'Pandas', 'Spark', and 'SQL' will link to the corresponding guides on how to configure a
 *     Datasource with that type of Data Connector. {true} will link to the "Connect to data: Overview"
 *     page.  {false} will not display anything.
 *   requireExpectationSuite: Valid values are {true} or {false}
 */
export default class Prerequisites extends React.Component {

  defaultPrerequisiteItems () {
    const returnItems = []
    if (this.props.requirePython === true) {
      returnItems.push(<li>An installation of Python {GxData.min_python} to {GxData.max_python}. To download and install Python, see <a href='https://www.python.org/downloads/'>Python downloads.</a></li>)
    }
    if (this.props.requireInstallation === true) {
      returnItems.push(<li>A Great Expectations instance. See <VersionedLink to='/oss/guides/setup/installation/install_gx'>Install Great Expectations with Data Source dependencies</VersionedLink>.</li>)
    }
    if (this.props.requireDataContext === true) {
      returnItems.push(<li><VersionedLink to='/oss/guides/setup/configuring_data_contexts/instantiating_data_contexts/instantiate_data_context'>A Data Context.</VersionedLink></li>)
    }
    if (this.props.requireSourceData === 'filesystem') {
      returnItems.push(<li>Access to data stored in a filesystem.</li>)
    } else if (this.props.requireSourceData === 'SQL') {
      returnItems.push(<li>Access to data stored in a SQL database.</li>)
    }
    if (this.props.requireDatasource === 'Pandas') {
      returnItems.push(<li><a href='/docs/oss/guides/connecting_to_your_data/fluent/filesystem/connect_filesystem_source_data'>A Data Source configured to access your Data Assets.</a></li>)
    } else if (this.props.requireDatasource === 'Spark') {
      returnItems.push(<li><a href='/docs/oss/guides/connecting_to_your_data/fluent/filesystem/connect_filesystem_source_data'>A Data Source configured to access your Data Assets.</a></li>)
    } else if (this.props.requireDatasource === 'SQL') {
      returnItems.push(<li><a href='/docs/oss/guides/connecting_to_your_data/fluent/database/connect_sql_source_data'>A Data Source configured to access your sData Assets.</a></li>)
    } else if (this.props.requireDatasource === true) {
      returnItems.push(<li><VersionedLink to='/oss/guides/connecting_to_your_data/connect_to_data_overview'>A Data Source configured to access your Data Assets</VersionedLink></li>)
    }
    if (this.props.requireExpectationSuite === true) {
      returnItems.push(<li><VersionedLink to='/oss/guides/expectations/create_expectations_overview'>A configured and saved Expectation Suite.</VersionedLink></li>)
    }

    return returnItems
  }

  render () {
    return (
      <div>
          <ul>
            {this.defaultPrerequisiteItems()}
          </ul>
          {this.props.children}
      </div>
    )
  }
}

Prerequisites.defaultProps = {
  requirePython: false,
  requireInstallation: false,
  requireDataContext: false,
  requireSourceData: null,
  requireDatasource: false,
  requireExpectationSuite: false
}
