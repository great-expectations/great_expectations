const visit = require('unist-util-visit');
const constructSnippetMap = require("./snippet")

const SNIPPET_MAP = constructSnippetMap("tests")

function codeImport() {
    return function transformer(tree, file) {
        const codes = [];
        const promises = [];

        visit(tree, 'code', (node, index, parent) => {
            codes.push([node, index, parent]);
        });

        for (const [node] of codes) {
            const nameMeta = (node.meta || '')
                .split(' ')
                .find(meta => meta.startsWith('name='));

            if (!nameMeta) {
                continue;
            }

            const res = /^name=(?<path>.+?)$/.exec(
                nameMeta
            );

            if (!res || !res.groups || !res.groups.path) {
                throw new Error(`Unable to parse named reference ${nameMeta}`);
            }

            let path = eval(res.groups.path)
            node.value = SNIPPET_MAP[path].contents
        }

        if (promises.length) {
            return Promise.all(promises);
        }
    };
}

module.exports = codeImport;
