import React from 'react'
import Highlight, { defaultProps } from 'prism-react-renderer'
defaultProps.language = 'python'

export class CodeSnippet extends React.Component {
  render () {
    return (
      <div style={{ }}>
        <Highlight {...defaultProps} code={this.props.code ? this.props.code.trim() : '# Please answer both questions'}>
          {({ className, style, tokens, getLineProps, getTokenProps }) => (
            <pre className={className} style={style}>
              {tokens.map((line, i) => (
                <div {...getLineProps({ line, key: i })} key={i}>
                  {line.map((token, key) => (
                    <span {...getTokenProps({ token, key })} key={i} />
                  ))}
                </div>
              ))}
            </pre>
          )}
        </Highlight>
      </div>
    )
  }
}
