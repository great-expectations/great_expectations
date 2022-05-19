---
title: How to test integrations
---

### Introduction
As the data stack ecosystem grows and expands in usage and tooling, so does the need to integrate with 3rd party
products or services. [Superconductive](https://superconductive.com) as drivers and ushers
of [Great Expectations](https://greatexpectations.io), we want to make the process to integrating with Great Expectations
as low friction as possible. We are committed to work and iterate in the process and greatly value any feedback you may have.
The aim of this document is to provide guidance for vendors or community partners which wish to integrate with us as to how to test said integrations within our testing infrastructure.

### Scope
While test requirements vary widly, we made some decisions to help us narrow down the cases we cover while keeping developer friction at a minimum.
To this end we have created a taxonomy of the types of tests that our self-service integration tests framework supports.

#### Type 1
Type 1 integration tests are those which are fully self-contained and can be or are already containerized. Our framework supports running tests in docker containers and(optionally) orchestrated via `docker compose`.

#### Type 2
Type 2 integration tests are those which depend on outside/third party services. For example, your integration depends on AWS Redshift, Snowflake, Databricks, etc. In this case
credentials for those will have to be provided to us (off-band). More about the specifics below.

## Steps for Adding Type 1 Tests

### 0. Reach out to our Developer Relations team
Before you embark in this journey, drop by and introduce yourself in the #integrations channel in our [Great Expectations Slack](https://greatexpectationstalk.slack.com)
to let us know. We're big believers of building strong relationships with ecosystem partners. And thus we believe
opening communication channels early in the process is essential.

### 1. Copy the template
Create a copy of `integration_template.md` and name it `integration_<my_product>.md`. This file is located in `great_expectations/docs/integrations/` directory.
This file is in markdown format and supports basic [docusaurus admonitions](https://docusaurus.io/docs/markdown-features/admonitions).

## Steps for Adding Type 2 Tests

### 0. Do this
fsdhfsdhfjkhsdjkfhjksdhfksd

### 1. Do that
fhsdkhfjksdfhjkdshfjkds

## Further assistance
 *  GE Slack #integrations channel
 *  Github issue (mention @devrel)
