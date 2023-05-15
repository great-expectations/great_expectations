#!/bin/bash

# Build API docs then build docusaurus docs.
# Currently used in our netlify pipeline.

print_orange () {
    ORANGE='\033[38;5;208m'
    NC='\033[0m' # No Color
    echo -e "${ORANGE}$1${NC}"
}

print_orange_line () {
    print_orange "================================================================================"
}

print_orange_header () {
    print_orange_line
    print_orange "$1"
    print_orange_line
}

CURRENT_COMMIT=$(git rev-parse HEAD)
# git pull to get the latest tags
git pull

print_orange_header "Copying previous versioned docs"
curl "https://superconductive-public.s3.us-east-2.amazonaws.com/oss_docs_versions_20230404.zip" -o "oss_docs_versions.zip"
unzip -oq oss_docs_versions.zip -d .

# Move versions.json outside of the repo so there are no conflicts when checking out earlier versions
VERSIONS_JSON_PATH=../../../versions.json
mv versions.json $VERSIONS_JSON_PATH

VERSIONS=$(cat $VERSIONS_JSON_PATH | python -c 'import json,sys;obj=json.load(sys.stdin);print(" ".join(obj))')

for version in $VERSIONS; do
  print_orange_header "Copying code referenced in docs from $version and writing to versioned_code/version-$version"

  git checkout "$version"
  git pull
  mkdir -p versioned_code/version-"$version"
  cp -r ../../tests versioned_code/version-"$version"
  cp -r ../../examples versioned_code/version-"$version"
  cp -r ../../great_expectations versioned_code/version-"$version"

done

print_orange_header "Prepare prior versions using the current commit (e.g. proposed commit if in a PR or develop if not)."
git checkout "$CURRENT_COMMIT"
git pull

# Update versioned code and docs

print_orange_header "Updating filepath in versioned docs"
# This is done in prepare_prior_versions.py
# Update filepath in versioned docs if they are using the old linenumber style of file=<filepath>L<lineno>
# by adding the correct versioned_code filepath e.g. versioned_code/version-0.14.13/<filepath>

print_orange_header "Updating snippet names in versioned docs and code"
# This is done in prepare_prior_versions.py
# Update snippet names in versioned docs if they are using the style of name="<snippet_name>"
# by prepending the version e.g. name="version-0.15.50 <original_snippet_name>"
# This is done in the docs and code so that the snippet processing tool can match up the correct snippet
# based on the version of the code file that existed when the document was released.
cd ../
python prepare_prior_versions.py
cd docusaurus

# Get latest released version from tag, check out to build API docs.
# Only if not PR deploy preview.
if [ "$PULL_REQUEST" == "false" ]
then
  GX_LATEST=$(git tag | grep -E "(^[0-9]{1,}\.)+[0-9]{1,}" | sort -V | tail -1)
  print_orange_header "Not in a pull request. Using latest released version ${GX_LATEST} at $(git rev-parse HEAD) to build API docs."
  git checkout "$GX_LATEST"
  git pull
else
  print_orange_header "Building from within a pull request, using the latest commit to build API docs so changes can be viewed in the Netlify deploy preview."
  git checkout "$CURRENT_COMMIT"
  git pull
fi

# Build current docs
print_orange_header "Installing Great Expectations library dev dependencies."
(cd ../../; pip install -c constraints-dev.txt -e ".[test]")

print_orange_header "Installing api docs dependencies."
(cd ../sphinx_api_docs_source; pip install -r requirements-dev-api-docs.txt)

print_orange_header "Building API docs for current version."
(cd ../../; invoke api-docs)

print_orange_header "Check back out current commit before building the rest of the docs."
git checkout "$CURRENT_COMMIT"
git pull

# Move versions.json back from outside of the repo
mv $VERSIONS_JSON_PATH versions.json
