import React from 'react'

import VersionedLink from '@site/src/components/VersionedLink'
import Prerequisites from './defaultPrerequisiteItems.jsx'

export default class InsPrerequisites extends Prerequisites {
  defaultPrerequisiteItems () {
    return [
      <li key={0.1}><VersionedLink to='/tutorials/quickstart'>Completed the Quickstart guide</VersionedLink></li>
    ]
  }
}
