#!/bin/bash

CHANGED_FILES=$(git diff HEAD origin/develop --name-only)
printf '%s\n' "[CHANGED FILES]" "${CHANGED_FILES[@]}" ""

DEPENDENCIES=$(python scripts/trace_docs_deps.py)
printf '%s\n' "[DEPENDENCIES]" "${DEPENDENCIES[@]}" ""

echo "File changes from 'great_expectations/' that impact 'docs/':"
for FILE in ${DEPENDENCIES}; do
    if [[ ${CHANGED_FILES[@]} =~ $FILE ]]; then
        echo "  Found change in local dependency:" $FILE
        echo "##vso[task.setvariable variable=DocsDependenciesChanged;isOutput=true]true"
    fi
done
