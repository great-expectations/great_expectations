import React from 'react'

/**
 * A flexible Prerequisites admonition block.
 * Styling/structure was copied over directly from docusaurus :::info admonition.
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
    return [
      <li key={0.1}><a href='/docs/tutorials/quickstart'>Completed the Quickstart guide</a></li>
    ]
  }

  render () {
    return (
      <div>
          <ul>
            {this.defaultPrerequisiteItems()}
            {this.extractMarkdownListItems().map((prereq, i) => (<li key={i}>{prereq}</li>))}
          </ul>
      </div>
    )
  }
}
