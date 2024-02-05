import React from 'react'

import Prerequisites from './defaultPrerequisiteItems.jsx'
import VersionedLink from '@site/src/components/VersionedLink'

export default class InsPrerequisites extends Prerequisites {
  defaultPrerequisiteItems () {
    return [
      <li key={0.1}><VersionedLink to='/oss/tutorials/quickstart'>Completed the Quickstart guide</VersionedLink></li>
    ]
  }
}
