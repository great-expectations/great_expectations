#!/usr/bin/env bash

aws --version
aws s3 ls s3://superconductive-public/static/gallery/ | grep old-prod

aws s3 cp s3://superconductive-public/static/gallery/expectation_library_v2--old-prod.json s3://superconductive-public/static/gallery/expectation_library_v2.json
aws s3 cp s3://superconductive-public/static/gallery/package_manifests--old-prod.json s3://superconductive-public/static/gallery/package_manifests.json
