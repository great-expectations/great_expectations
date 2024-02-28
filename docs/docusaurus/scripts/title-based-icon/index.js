/*
This custom script adds icons to the code snippets.
It adds a file icon by default, unless the title contains the word "output",
in which case an output terminal icon will be used.
*/
const visit = require('unist-util-visit')

function titleBasedIcon () {

    return function transformer (tree, file) {
        const codes = []

        // Walk the AST of the markdown file and filter for code snippets
        visit(tree, 'code', (node, index, parent) => {
            codes.push([node, index, parent])
        })

        for (const [node] of codes) {
            const meta = node.meta || ''

            let className = 'with-file-icon'

            if (meta) {
                const titleMeta = /\btitle=(?<snippetTitle>["'].+["'])/.exec(
                    meta
                )

                if (titleMeta && titleMeta.groups.snippetTitle.toLowerCase().includes('output')) {
                    className = 'with-terminal-output-icon'
                }
            }

            node.data = {
                hProperties: {
                    className: className
                }
            }
        }
    }
}

module.exports = titleBasedIcon
