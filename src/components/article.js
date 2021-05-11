import React from 'react'

export class Article extends React.Component {
  shouldBeHidden () {
    return this.props.tags.some(tag => this.props.hiddenTags.includes(tag))
  }

  render () {
    return (
      <li style={{
        fontWeight: 'bold',
        fontSize: '17px',
        display: this.shouldBeHidden() ? 'none' : 'block'
      }}
      >
        <a href={this.props.url ? this.props.url : '#'}>{this.props.title}</a>
      </li>
    )
  }
}
