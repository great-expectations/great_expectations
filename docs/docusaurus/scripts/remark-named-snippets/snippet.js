const fs = require('fs')
const glob = require('glob')
const htmlparser2 = require('htmlparser2')

/**
 * Constructs a map associating names with snippets by parsing source code.
 *
 * @param {string} dirs - The directories of source code to traverse when constructing the map.
 * @returns {object} The "snippet map", which is an object with name, snippet key-value pairs.
 */
function constructSnippetMap(dirs) {
  const snippets = parseSourceDirectories(dirs)

  const snippetMap = {}
  for (const snippet of snippets) {
    const name = snippet.name
    if (name in snippetMap) {
      throw new Error(
        `A snippet named ${name} has already been defined elsewhere`
      )
    }
    delete snippet.name // Remove duplicate filename to clean up stdout
    snippetMap[name] = snippet
  }

  return snippetMap
}

/**
 * Parses input directories of source code using an HTML parser to collect snippets.
 *
 * @param {string} dirs - The directories to parse for snippet definitions.
 * @returns {object[]} A list of snippet objects parsed from the input directory.
 */
function parseSourceDirectories(dirs) {
  const files = []
  for (const dir of dirs) {
    for (const file of glob.sync(dir + '/**/*.{py,yml,yaml}')) {
      files.push(file)
    }
  }

  const allSnippets = []
  for (const file of files) {
    const snippets = parseFile(file)
    for (const snippet of snippets) {
      allSnippets.push(snippet)
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
  const data = fs.readFileSync(file, 'utf8')

  // The stack here is used to deal with nested snippets.
  // Everytime we see an open tag, we create a new snippet on the stack.
  const stack = []

  // Once the top snippet on the stack recieves an end tag, we pop it from the stack
  // and add it to our result snippets.
  let snippets = []

  const parser = new htmlparser2.Parser({
    onopentag(tagname, attrs) {
      // if snippet at the top of stack doesn't have content
      if (tagname !== 'snippet') {
        // If we see a non-snippet tag, we want to make sure we still append the literal text to our parsed results.
        // This is particularly relevant around regex in our docs
        this.ontext(`<${tagname}>`)
        return
      }

      const snippetName = attrs.name
      stack.push({ name: snippetName, file: file, contents: '' })
    },
    ontext(text) {
      if (stack.length === 0) {
        return
      }

      // If we have nested snippets, all snippets on the stack need to be updated
      // to reflect any nested content
      for (let i = 0; i < stack.length; i++) {
        stack[i].contents += text
      }
    },
    onclosetag(tagname) {
      if (tagname !== 'snippet') {
        return
      }

      if (stack.length === 0) {
        throw new Error(`Closing tag found before any opening tags in ${file}`)
      }

      const popped = stack.pop()
      // Clean up formatting and identation before writing to result array
      popped.contents = sanitizeText(popped.contents)
      snippets.push(popped)
    }
  })
  parser.write(data)
  parser.end()

  snippets = snippets.filter((s) => s.name)

  return snippets
}

/**
 * Strips any unnecessary whitespace and source code comments from the ends of the input string.
 * Additionally, removes any nested snippet tags (as applicable).
 *
 * @param {string} text - The text to be sanitized.
 * @returns {string} The sanitized string.
 */
function sanitizeText(text) {
  // Remove leading carriage return
  if (text.startsWith('\n') || text.startsWith('\r')) {
    text = text.substring(1, text.length)
  }

  // Calculate indentation to remove
  let indent = ''
  for (let i = 0; i < text.length; i++) {
    if (text[i] === ' ') {
      indent += ' '
    } else {
      break
    }
  }

  // Apply unintent and misc cleanup
  function unindent(line) {
    if (line.startsWith(indent)) {
      line = line.substring(indent.length, text.length)
    }
    return line
  }

  return text
    .split('\n')
    .filter((l) => !(l.trim() === '#')) // Remove any nested snippet remnants
    .map(unindent)
    .join('\n')
    .trim()
}

/**
 * Organize parsed snippets by source filename.
 * If provided, input filenames will filter this output.
 *
 * Note that this is what is run if this file is invoked by Node.
 * An alias `yarn snippet-check` is defined in `package.json` for convenience.
 */
function getDirs() {
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

function processVerbose() {
  const args = process.argv.slice(2)

  let verbose = false
  if (args.includes('verbose') || args.includes('-v') || args.includes('--verbose')) {
    verbose = true
    console.log('Using verbose mode. Printing all snippets.')
  }
  return verbose
}

function main(verbose = false) {
  const snippets = parseSourceDirectories(getDirs())
  let argNum = 2
  if (verbose) { argNum = 3 }
  const targetFiles = process.argv.slice(argNum)

  const out = {}
  for (const snippet of snippets) {
    // If no explicit args are provided, default to all snippets
    // Else, ensure that the snippet's source file was requested by the user
    const file = snippet.file
    if (targetFiles.length > 0 && !(targetFiles.includes(file))) {
      continue
    }
    if (!(file in out)) {
      out[file] = []
    }
    delete snippet.file // Remove duplicate filename to clean up stdout
    out[file].push(snippet)
  }
  if (verbose) {
    console.log(out)
  }
}

if (require.main === module) {
  const verbose = processVerbose()
  main(verbose)
}

module.exports = constructSnippetMap
