import React from 'react'

import Prerequisites from './defaultPrerequisiteItems.jsx'

export default class InsPrerequisites extends Prerequisites {
  defaultPrerequisiteItems () {
    return [
      <li key={0.1}><a href='/docs/tutorials/quickstart'>Completed the Quickstart guide</a></li>
    ]
  }
}
