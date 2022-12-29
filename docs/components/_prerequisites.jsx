import React from 'react'
import Admonition from '@theme/Admonition'

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
  extractMarkdownListItems () {
    try {
      const children = React.Children.toArray(this.props.children).map((item) => (item.props.children))
      const listItems = React.Children.toArray(children).map((item) => (item.props.children))
      return listItems
    } catch (error) {
      const message = '🚨 The Prerequisites component only accepts markdown list items 🚨'
      console.error(message, error)
      window.alert(message)
      return [message]
    }
  }

  defaultPrerequisiteItems () {
    const returnItems = [
      <li key={0.1}>
        Completed the <a href='/docs/tutorials/getting_started/tutorial_overview'>Getting Started Tutorial</a>
      </li>]
    if (this.props.requireInstallation === true) {
      returnItems.push(<li>Set up an <a href='/docs/guides/setup/installation/local'>installation of Great Expectations</a></li>)
    }
    if (this.props.requireDataContext === true) {
      returnItems.push(<li><a href='/docs/guides/setup/configuring_data_contexts/how_to_configure_a_new_data_context_with_the_cli'>Created your Data Context</a></li>)
    }
    if (this.props.requireSourceData === 'filesystem') {
      returnItems.push(<li>Access to data stored in a filesystem.</li>)
    } else if (this.props.requireSourceData === 'SQL') {
      returnItems.push(<li>Access to data stored in a SQL database.</li>)
    }
    if (this.props.requireDatasource === 'Pandas') {
      returnItems.push(<li><a href='/docs/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_pandas_datasource'>Configured a Datasource to access your source data</a></li>)
    } else if (this.props.requireDatasource === 'Spark') {
      returnItems.push(<li><a href='/docs/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_spark_datasource'>Configured a Datasource to access your source data</a></li>)
    } else if (this.props.requireDatasource === 'SQL') {
      returnItems.push(<li><a href='/docs/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_sql_datasource'>Configured a Datasource to access your source data</a></li>)
    } else if (this.props.requireDatasource === true) {
      returnItems.push(<li><a href='/docs/guides/connecting_to_your_data/connect_to_data_overview'>Configured a Datasource to access your source data</a></li>)
    }
    if (this.props.requireExpectationSuite === true) {
      returnItems.push(<li><a href='/docs/guides/expectations/create_expectations_overview'>Configured and saved an Expectation Suite</a></li>)
    }

    return returnItems
  }

  render () {
    return (
      <div>
        <Admonition type='caution' title='Prerequisites'>
          <h5>This guide assumes you have:</h5>
          <ul>
            {this.defaultPrerequisiteItems()}
            {this.extractMarkdownListItems().map((prereq, i) => (<li key={i}>{prereq}</li>))}
          </ul>
        </Admonition>
      </div>
    )
  }
}

Prerequisites.defaultProps = {
  requireInstallation: false,
  requireDataContext: false,
  requireSourceData: null,
  requireDatasource: false,
  requireExpectationSuite: false
}
