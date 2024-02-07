
/**
 * @returns {string[]} A list of directories that should contain snippets, relative do docs/docusaurus/.
 */
function getDirs () {
  return ['./docs', './versioned_docs']
}

module.exports = {
    getDirs,
}