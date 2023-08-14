/*
This script enables name-based snippet retrieval in Docusaurus-enabled docs using
the following syntax:
```
    ```python name="getting_started_imports"
```

This pattern is directly inspired by remark-code-import, which references using line numbers.

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
const glob = require('glob')
const constructSnippetMap = require('./snippet')

function getDirs () {
  // Get all directories that should be processed
  const manualDirs = ['../../great_expectations', '../../tests']
  const versionDirs = glob.sync('versioned_code/*/')
  // remove v0.14.13 from processing since it does not use named snippets
  const index = versionDirs.indexOf('versioned_code/version-0.14.13/')
  if (index !== -1) {
    versionDirs.splice(index, 1)
  }
  return manualDirs.concat(versionDirs)
}

function codeImport () {
  // Instantiated within the import so it can be hot-reloaded
  const snippetMap = constructSnippetMap(getDirs())

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
    }

    if (promises.length) {
      return Promise.all(promises)
    }
  }
}

module.exports = codeImport