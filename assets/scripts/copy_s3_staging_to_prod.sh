#!/usr/bin/env bash

aws --version
aws s3 ls s3://superconductive-public | grep staging

aws s3 cp s3://superconductive-public/expectation_library_v2--staging.json s3://superconductive-public/expectation_library_v2--tmp.json
aws s3 cp s3://superconductive-public/package_manifests--staging.json s3://superconductive-public/package_manifests--tmp.json
