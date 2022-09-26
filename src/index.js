const fs = require('fs');
const glob = require('glob');
const visit = require('unist-util-visit');
const htmlparser2 = require("htmlparser2");

function constructSnippetMap(dir) {
    let snippets = parseDirectory(dir)

    let snippetMap = {}
    for (let i in snippets) {
        let snippet = snippets[i]
        snippetMap[snippet.name] = snippet
    }

    return snippetMap
}

function parseDirectory(dir) {
    let files = glob.sync(dir + "/**/*.py")

    let allSnippets = []
    for (let i in files) {
        let snippets = parseFile(files[i])
        for (let i in snippets) {
            allSnippets.push(snippets[i])
        }
    }

    return allSnippets
}

function parseFile(file) {
    let data = fs.readFileSync(file, 'utf8');

    let snippets = []
    let stack = []

    const parser = new htmlparser2.Parser({
        onopentag(tagname, attrs) {
            if (tagname != "snippet") {
                return
            }

            let snippetName = attrs["name"]
            if (!snippetName) {
                return
            }

            stack.push({ "name": snippetName, "file": file, "contents": "" })
        },
        ontext(text) {
            if (stack.length == 0) {
                return
            }
            stack[stack.length - 1].contents = text;
        },
        onclosetag(tagname) {
            if (tagname != "snippet") {
                return
            }

            let popped = stack.pop();
            if (popped) {
                snippets.push(popped)
            }
        },
    });
    parser.write(data);
    parser.end();

    return snippets
}

const snippetMap = constructSnippetMap("tests")

function codeImport(options = {}) {
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
            node.value = snippetMap[path].contents
        }

        if (promises.length) {
            return Promise.all(promises);
        }
    };
}

module.exports = codeImport;
