import React from 'react'
import VersionedLink from '@site/src/components/VersionedLink'

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
  defaultPrerequisiteItems () {
    return [
      <li key={0.1}><VersionedLink to='/oss/tutorials/quickstart'>Completion of the Quickstart guide.</VersionedLink></li>,
      <li key={0.2}><VersionedLink to='/oss/guides/setup/setup_overview'>A working installation of Great Expectations.</VersionedLink></li>
    ]
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

