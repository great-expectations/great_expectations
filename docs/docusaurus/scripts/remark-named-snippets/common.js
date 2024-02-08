const glob = require('glob')

/**
 * @returns {string[]} A list of directories that should contain snippets, relative do docs/docusaurus/.
 */
function getDirs () {
  const currentDocs = 'docs'
  const versionedDocs = glob.sync('versioned_docs/*')
  return [currentDocs, ...versionedDocs]
}

module.exports = {
    getDirs,
}
