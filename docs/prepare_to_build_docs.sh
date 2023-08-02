#!/bin/bash

# Build API docs then build docusaurus docs.
# Currently used in our netlify pipeline.
# this should trigger something
source ../logging.sh

CURRENT_COMMIT=$(git rev-parse HEAD)
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
# git pull to get the latest tags
git pull

S3_URL="https://superconductive-public.s3.us-east-2.amazonaws.com/oss_docs_versions_20230615.zip"
print_orange_header "Copying previous versioned docs from $S3_URL"
curl $S3_URL -o "oss_docs_versions.zip"
unzip -oq oss_docs_versions.zip -d .
rm oss_docs_versions.zip

VERSIONS=$(cat versions.json | python -c 'import json,sys;obj=json.load(sys.stdin);print(" ".join(obj))')
mkdir versioned_code

for version in $VERSIONS; do
  print_orange_header "Copying code referenced in docs from $version and writing to versioned_code/version-$version"

  curl -LJO "https://github.com/great-expectations/great_expectations/archive/refs/tags/$version.zip"
  unzip -oq "great_expectations-$version.zip" -d versioned_code
  mv versioned_code/"great_expectations-$version" versioned_code/version-"$version"
  rm "great_expectations-$version.zip"

done


print_orange_header "Updating versioned code and docs via prepare_prior_versions.py..."

cd ../
python prepare_prior_versions.py
cd docusaurus

print_orange_header "Updated versioned code and docs"

# Get latest released version from tag, check out to build API docs.
# Only if not PR deploy preview.
if [ "$PULL_REQUEST" == "false" ]
then
  GX_LATEST=$(git tag | grep -E "(^[0-9]{1,}\.)+[0-9]{1,}" | sort -V | tail -1)
  git checkout "$GX_LATEST"
  git pull
  print_orange_header "Not in a pull request. Using latest released version ${GX_LATEST} at $(git rev-parse HEAD) to build API docs."
else
  print_orange_header "Building locally or from within a pull request, using the latest commit to build API docs so changes can be viewed in the Netlify deploy preview."
fi

# Build current docs
print_orange_header "Installing Great Expectations library dev dependencies."
(cd ../../; pip install -c constraints-dev.txt -e ".[test]")

print_orange_header "Installing api docs dependencies."
(cd ../sphinx_api_docs_source; pip install -r requirements-dev-api-docs.txt)

print_orange_header "Building API docs for current version. Please ignore sphinx docstring errors in red/pink, for example: ERROR: Unexpected indentation."
(cd ../../; invoke api-docs)

# Check out the current branch if building locally, otherwise if building in Netlify check out
# the current commit.
if [[ -z "${PULL_REQUEST}" ]]
then
  print_orange_header "Building locally - Checking back out current branch (${CURRENT_BRANCH}) before building the rest of the docs."
  git checkout "$CURRENT_BRANCH"
else
  print_orange_header "In a pull request or deploying in netlify (PULL_REQUEST = ${PULL_REQUEST}) Checking out ${CURRENT_COMMIT}."
  git checkout "$CURRENT_COMMIT"
fi

git pull
