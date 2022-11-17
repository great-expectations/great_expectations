/*
This script enables name-based snippet retrieval in Docusaurus-enabled docs using
the following syntax:
```
    ```python name="getting_started_imports"
```

This pattern is directly inspired by remark-code-import, which references using line numbers:
```
    ```python file=../../tests/integration/docusaurus/tutorials/getting-started/getting_started.py#L1-L5
```

As snippets are bound by identifier and not specific line numbers, they are far less susceptible
to breakage when docs and source code are being updated.

Named snippets are defined with the following syntax:
```
    # <snippet name="getting_started_imports">
    import great_expectations as gx
    ...
    # </snippet>
```
*/
const visit = require('unist-util-visit')
const constructSnippetMap = require('./snippet')

function codeImport () {
  // Instantiated within the import so it can be hot-reloaded
  const snippetMap = constructSnippetMap('.')
  console.log(snippetMap)

  return function transformer (tree, file) {
    const codes = []
    const promises = []

    // Walk the AST of the markdown file and filter for code snippets
    visit(tree, 'code', (node, index, parent) => {
      codes.push([node, index, parent])
    })

    for (const [node] of codes) {
      const meta = node.meta || ''
      if (!meta) {
        continue
      }

      const nameMeta = /^name=(?<snippetName>.+?)$/.exec(
        meta
      )
      if (!nameMeta) {
          continue
      }

      let name = nameMeta.groups.snippetName
      if (!name) {
        throw new Error(`Unable to parse named reference ${nameMeta}`)
      }

      // Remove any surrounding quotes
      name = name.replaceAll("'", '').replaceAll('"', '')
      if (!(name in snippetMap)) {
        throw new Error(`Could not find any snippet named ${name}`)
      }

      node.value = snippetMap[name].contents
      console.log(`Substituted value for named snippet "${name}"`)
    }

    if (promises.length) {
      return Promise.all(promises)
    }
  }
}

module.exports = codeImport
