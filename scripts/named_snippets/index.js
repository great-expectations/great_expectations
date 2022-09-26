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
const visit = require('unist-util-visit');
const constructSnippetMap = require("./snippet")

const SNIPPET_MAP = constructSnippetMap("tests")

function codeImport() {
    return function transformer(tree, file) {
        const codes = [];
        const promises = [];

        // Walk the AST of the markdown file and filter for code snippets
        visit(tree, 'code', (node, index, parent) => {
            codes.push([node, index, parent]);
        });

        for (const [node] of codes) {
            // Syntax: ```python name="my_python_snippet"
            const nameMeta = (node.meta || '')
                .split(' ')
                .find(meta => meta.startsWith('name='));

            if (!nameMeta) {
                continue;
            }

            const res = /^name=(?<snippetName>.+?)$/.exec(
                nameMeta
            );

            let name = res.groups.snippetName
            if (!name) {
                throw new Error(`Unable to parse named reference ${nameMeta}`);
            }

            name = eval(name) // Remove any surrounding quotes
            if (!(name in SNIPPET_MAP)) {
                throw new Error(`Could not find any snippet named ${name}`)
            }
            node.value = SNIPPET_MAP[name].contents
        }

        if (promises.length) {
            return Promise.all(promises);
        }
    };
}

module.exports = codeImport;
