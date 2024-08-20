[![Python Versions](https://img.shields.io/pypi/pyversions/great_expectations.svg)](https://pypi.python.org/pypi/great_expectations)
[![PyPI](https://img.shields.io/pypi/v/great_expectations)](https://pypi.org/project/great-expectations/#history)
[![PyPI Downloads](https://img.shields.io/pypi/dm/great-expectations)](https://pypistats.org/packages/great-expectations)
[![Build Status](https://img.shields.io/azure-devops/build/great-expectations/bedaf2c2-4c4a-4b37-87b0-3877190e71f5/1)](https://dev.azure.com/great-expectations/great_expectations/_build/latest?definitionId=1&branchName=develop)
[![pre-commit.ci Status](https://results.pre-commit.ci/badge/github/great-expectations/great_expectations/develop.svg)](https://results.pre-commit.ci/latest/github/great-expectations/great_expectations/develop)
[![codecov](https://codecov.io/gh/great-expectations/great_expectations/graph/badge.svg?token=rbHxgTxYTs)](https://codecov.io/gh/great-expectations/great_expectations)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.5683574.svg)](https://doi.org/10.5281/zenodo.5683574)
[![Twitter Follow](https://img.shields.io/twitter/follow/expectgreatdata?style=social)](https://twitter.com/expectgreatdata)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://greatexpectations.io/slack)
[![Contributors](https://img.shields.io/github/contributors/great-expectations/great_expectations)](https://github.com/great-expectations/great_expectations/graphs/contributors)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v1.json)](https://github.com/charliermarsh/ruff)

<!-- <<<Super-quickstart links go here>>> -->

<img align="right" src="./docs/docusaurus/static/img/gx-mark-160.png">

## About GX Core

GX Core is a data quality platform designed by and for data engineers. It helps you surface issues quickly and clearly while also making it easier to collaborate with nontechnical stakeholders.

Its powerful technical tools start with Expectations: expressive and extensible unit tests for your data. As you create and run tests, your test definitions and results are automatically rendered in human-readable plain-language Data Docs.

Expectations and Data Docs create verifiability and clarity throughout your data quality process. That means you can spend less time translating your work for others, and more time achieving real mutual understanding across your entire organization.

Data science and data engineering teams use GX OSS to:

- Validate data they ingest from other teams or vendors.
- Test data for correctness post-transfomation.
- Proactively prevent low-quality data from moving downstream and becoming visible in data products and applications.
- Streamline knowledge capture from subject-matter experts and make implicit knowledge explicit.
- Develop rich, shared documentation of their data.

Learn more about how data teams are using GX OSS in [case studies from Great Expectations](https://greatexpectations.io/case-studies/).

See [Down with pipeline debt](https://greatexpectations.io/blog/down-with-pipeline-debt-introducing-great-expectations/) for an introduction to our pipeline data quality testing philosophy.

## Our upcoming 1.0 release

We’re planning a ton of work to take GX OSS to the next level as we move to 1.0!

Our biggest goal is to improve the user and contributor experiences by **streamlining the API**, based on the feedback we’ve received from the community (thank you!) over the years.

Learn more about our plans for 1.0 and how we’ll be making this transition in our [blog post](https://greatexpectations.io/blog/the-next-step-for-gx-oss-1-0).

<!--
--------------------------------------------------
<<<A bunch of logos go here for social proof>>>

--------------------------------------------------
-->

## Integration support policy

GX OSS supports Python `3.8` through `3.11`.
Experimental support for Python `3.12` and later can be enabled by setting a `GX_PYTHON_EXPERIMENTAL` environment variable when installing `great_expectations`.

For data sources and other integrations that GX supports, see [GX integration support policy](https://docs.greatexpectations.io/docs/application_integration_support) for additional information.

## Get started

GX recommends deploying GX OSS within a virtual environment. For more information about getting started with GX OSS, see [Get started with Great Expectations](https://docs.greatexpectations.io/docs/oss/tutorials/quickstart).

1. Run the following command in an empty base directory inside a Python virtual environment to install GX OSS:

	```bash title="Terminal input"
	pip install great_expectations
	```
2. Run the following command to import the `great_expectations module` and create a Data Context:

	```python
	import great_expectations as gx

	context = gx.get_context()
	```

## Get support from GX and the community

They are listed in the order in which GX is prioritizing the support issues:

1. Issues and PRs in the [GX GitHub repository](https://github.com/great-expectations)
2. Questions posted to the [GX Core Discourse forum](https://discourse.greatexpectations.io/c/oss-support/11)
3. Questions posted to the [GX Slack community channel](https://greatexpectationstalk.slack.com/archives/CUTCNHN82)

## Contribute
We deeply value the contributions and engagement of our community. We're now accepting PRs for bug fixes. To ensure quality, we're not yet ready to accept feature contributions in parts of code base where there is not clear API for extensions.  However, we're actively working to increase that surface area. Thank you for your patience and being a crucial part of our data quality journey!

### Levels of contribution readiness
🟢 Ready. Have a clear and public API for extensions.
🟡 Partially ready. Case-by-case.
🔴 Not ready. Will accept contributions that fix existing bugs or workflows.

| GX Component         | Readiness          | Notes |
| -------------------- | ------------------ | ----- |
| Action               | 🟢 Ready           |       |
| CredentialStore      | 🟢 Ready           |       |
| BatchDefinition      | 🟡 Partially ready | Formerly known as splitters |
| DataSource           | 🔴 Not ready       | Includes MetricProvider and ExecutionEngine |
| DataContext          | 🔴 Not ready       | Also known as Configuration Stores |
| DataAsset            | 🔴 Not ready       |       |
| Expectation          | 🔴 Not ready       |       |
| ValidationDefinition | 🔴 Not ready       |       |
| Checkpoint           | 🔴 Not ready       |       |
| CustomExpectations   | 🔴 Not ready       |       |
| Data Docs            | 🔴 Not ready       | Also known as Renderers |


## Code of conduct
Everyone interacting in GX OSS project codebases, Discourse forums, Slack channels, and email communications is expected to adhere to the [GX Community Code of Conduct](https://discourse.greatexpectations.io/t/gx-community-code-of-conduct/1199).
