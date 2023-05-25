#!/usr/bin/env bash

aws --version
aws s3 ls s3://superconductive-public/static/gallery/ | grep "_full"

aws s3 cp s3://superconductive-public/static/gallery/ . --recursive --exclude "*" --include "*_full.json"
