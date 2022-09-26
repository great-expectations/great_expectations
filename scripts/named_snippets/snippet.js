const fs = require('fs');
const glob = require('glob');
const htmlparser2 = require("htmlparser2");

/**
 * Constructs a map associating names with snippets by parsing source code.
 *
 * @param {string} dir - The directory of source code to traverse when constructing the map.
 * @returns {object} The "snippet map", which is an object with name, snippet key-value pairs.
 */
function constructSnippetMap(dir) {
    let snippets = parseDirectory(dir)

    let snippetMap = {}
    for (let i in snippets) {
        let snippet = snippets[i]
        let name = snippet.name
        if (name in snippetMap) {
            throw new Error(`A snippet named ${name} has already been defined elsewhere`)
        }
        snippetMap[name] = snippet
    }

    console.log(snippetMap)
    return snippetMap
}

/**
 * Parses an input directory using an HTML parser to collect snippets.
 *
 * @param {string} dir - The directory to parse for snippet definitions.
 * @returns {object[]} A list of snippet objects parsed from the input directory.
 */
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

/**
 * Parses an input file using an HTML parser to collect user-defined snippets.
 *
 * @param {string} file - The file to parse for snippet definitions.
 * @returns {object[]} A list of snippet objects parsed from the input file.
 */
function parseFile(file) {
    let data = fs.readFileSync(file, 'utf8');

    // The stack here is used to deal with nested snippets.
    // Everytime we see an open tag, we create a new snippet on the stack.
    let stack = []

    // Once the top snippet on the stack recieves an end tag, we pop it from the stack
    // and add it out our result snippets.
    let snippets = []

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

    let length = snippets.length;
    if (length) {
        console.log(`Collected ${length} reference(s) from ${file}`)
    }
    return snippets
}

/**
 * Strips any unnecessary whitespace and source code comments from the input string.
 *
 * @param {string} text - The text to be sanitized.
 * @returns {string} The sanitized string.
 */
function sanitize_text(text) {
    text = text.trim()
    if (text.endsWith("#")) {
        text = text.substring(0, text.length - 1)
    }
    text = text.trim()
    return text
}


module.exports = constructSnippetMap;
