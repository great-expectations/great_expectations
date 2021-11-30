---
title: How to use the Great Expectations Docker images
---

import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx'

This guide will help you use the official Great Expectations Docker images.
This is useful if you wish to have a fully portable Great Expectations runtime that can be used locally or deployed on the cloud.

<Prerequisites>

- Installed Docker on your machine

</Prerequisites>

## Steps

### 1. Choose Docker image

First, choose which image you'd like to use by browsing the offical [Great Expectations Docker image registry](https://hub.docker.com/r/greatexpectations/great_expectations/tags).

**Note**: We do not use the `:latest` tag, so you will need to specify an exact tag.

### 2. Pull the Docker image

For example:
```bash
docker pull greatexpectations/great_expectations:python-3.7-buster-ge-0.12.0
```

### 3. Mount the great_expectations directory

Next, we assume you have a Great Expectations project deployed at ``/full/path/to/your/project/great_expectations``. You need to mount the local ``great_expectations`` directory into the container at ``/usr/app/great_expectations``, and from there you can run all non-interactice commands, such as running checkpoints and listing items:

```bash
docker run \
-v /full/path/to/your/project/great_expectations:/usr/app/great_expectations \
greatexpectations/great_expectations:python-3.7-buster-ge-0.12.0 \
datasource list
```

## Additional notes

If you need to run interactive ``great_expectations`` commands, you can simply add the `-it` flags for interactive mode.

```bash
docker run -it \
-v /full/path/to/your/project/great_expectations:/usr/app/great_expectations \
greatexpectations/great_expectations:python-3.7-buster-ge-0.12.0
```
