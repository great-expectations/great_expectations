#!/usr/bin/env bash

aws --version
aws s3 ls s3://superconductive-public/static/gallery/ | grep staging

aws s3 cp s3://superconductive-public/static/gallery/expectation_library_v2--staging.json s3://superconductive-public/static/gallery/expectation_library_v2.json
aws s3 cp s3://superconductive-public/static/gallery/package_manifests--staging.json s3://superconductive-public/static/gallery/package_manifests.json
