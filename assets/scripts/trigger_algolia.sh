#!/usr/bin/env bash

which npm
which node
which yarn

if [[ ! -d "AlgoliaScripts" ]]; then
    echo "The AlgoliaScripts directory isn't in this branch yet"
    exit 1
fi

cd "AlgoliaScripts"
if [[ ! -d "node_modules" ]]; then
    npm install || exit 1
fi

node ./upload_s3_expectation_to_algolia.js
node ./upload_s3_packages_to_algolia.js
