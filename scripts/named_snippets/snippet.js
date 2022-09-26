const fs = require('fs');
const glob = require('glob');
const htmlparser2 = require("htmlparser2");

function constructSnippetMap(dir) {
    let snippets = parseDirectory(dir)

    let snippetMap = {}
    for (let i in snippets) {
        let snippet = snippets[i]
        let name = snippet.name
        if (name in snippetMap) {
            throw new Error()
        }
        snippetMap[name] = snippet
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
            stack[stack.length - 1].contents = sanitize_text(text);
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

function sanitize_text(text) {
    return text
}


module.exports = constructSnippetMap;
