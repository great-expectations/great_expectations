#!/bin/bash

source ../logging.sh

print_orange_header "Preparing to build docs..."
../prepare_to_build_docs.sh

print_orange_header "Running yarn start to serve docs locally..."
yarn start
