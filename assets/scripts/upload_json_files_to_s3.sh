#!/usr/bin/env bash

aws --version
aws s3 ls s3://superconductive-public/static/gallery/ | grep -E "(_full|expectation_library)"

aws s3 cp . s3://superconductive-public/static/gallery/ --exclude "*" --include "*.json"
