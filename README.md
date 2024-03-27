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

## About GX OSS

GX OSS is a data quality platform designed by and for data engineers. It helps you surface issues quickly and clearly while also making it easier to collaborate with nontechnical stakeholders.

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

## Get started

GX recommends deploying GX OSS within a virtual environment. For more information about getting started with GX OSS, see [Get started with Great Expectations](https://docs.greatexpectations.io/docs/oss/guides/setup/get_started_lp).

1. Run the following command in an empty base directory inside a Python virtual environment to install GX OSS:

    ```bash title="Terminal input"
    pip install great_expectations
    ```
2. Run the following command to import the `great_expectations module` and create a Data Context:

	```python
	import great_expectations as gx

	context = gx.get_context()
	```

## Get support

- Chat with community members about general issues and seek solutions on the [GX Slack community channel](https://greatexpectationstalk.slack.com/archives/CUTCNHN82).

- If you've found a bug, open an issue in one of the [GX GitHub repositories](https://github.com/great-expectations).

## Contribute

We deeply value the contributions and engagement of our community. We’re temporarily pausing the acceptance of new pull requests (PRs). We’re going to be updating the API and codebase frequently and significantly over the next few months—we don’t want contributors to spend time and effort only to find that we’ve just implemented a breaking change for their work.

Hold onto your fantastic ideas and PRs until after the 1.0 release, when we will be excited to resume accepting them. We appreciate your understanding and support as we make this final push toward this exciting milestone. Watch for updates in our [Slack](https://greatexpectations.io/slack) community, and thank you for being a crucial part of our journey!

## Code of conduct

Everyone interacting in GX OSS project codebases, Discourse forums, Slack channels, and email communications is expected to adhere to the [GX Community Code of Conduct](https://discourse.greatexpectations.io/t/gx-community-code-of-conduct/1199).
