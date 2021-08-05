import React from 'react'

import Prerequisites from './defaultPrerequisiteItems.jsx'

export default class InsPrerequisites extends Prerequisites {
  defaultPrerequisiteItems () {
    return [
      <li key={0.1}>Completed the <a href='/docs/tutorials/getting_started/intro'>Getting Started Tutorial</a></li>
    ]
  }
}
