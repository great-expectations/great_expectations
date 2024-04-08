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
const { visit } = require('unist-util-visit')
const path = require('path')
const constructSnippetMap = require('./snippet')
const { getDirs } = require('./common')

function codeImport () {
  // Instantiated within the import so it can be hot-reloaded
  const dirs = getDirs()
  const snippetMapsByNamespace = {}
  for (let dir of dirs) {
    snippetMapsByNamespace[dir] = constructSnippetMap(dir)
  }

  return function transformer (tree, file) {
    const codes = []
    const promises = []
    const namespace = getFileNamespace(file, dirs)
    const snippetMap = snippetMapsByNamespace[namespace]

    // Walk the AST of the markdown file and filter for code snippets
    visit(tree, 'code', (node, index, parent) => {
      codes.push([node, index, parent])
    })

    for (const [node] of codes) {
      const meta = node.meta || ''
      if (!meta) {
        continue
      }

      const nameMeta = /\bname=(?<snippetName>["'].+["'])/.exec(
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

/**
 * Gets what we'll call the "namespace" of the file, e.g. docs, versioned_docs/<VERSION>, etc
 * @param {VFile} file 
 * @param {string[]} namespaces
 * @returns 
 */
function getFileNamespace(file, namespaces) {
  const relativePath = path.relative(file.cwd, file.path);

  for (const namespace of namespaces) {
    if (relativePath.startsWith(namespace)) {
      return namespace
    }
  }
  throw Error(`No namespace found for file ${file.path} with namespaces ${namespaces}`)
}

module.exports = codeImport