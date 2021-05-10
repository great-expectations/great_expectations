import React from 'react'

export class Prerequisites extends React.Component {
  render () {
    return (
      <div style={{ display: 'block', background: '#eee', padding: '20px', margin: '1em 0', borderRadius: '10px' }}>
        <h3>Prerequisites :: This guide assumes you have:</h3>
        <ul>
          <li>Complete the <a href='/docs/tutorials/getting-started/intro'>Getting Started Tutorial</a></li>
          {this.props.deps.map((dep, i) => (<li key={i}>install <strong>{dep.name}</strong> by running <strong>{dep.pip}</strong>.</li>))}
          {this.props.notes.map((note, i) => (<li key={'dep-' + i}>{note}</li>))}
        </ul>
      </div>
    )
  }
}
