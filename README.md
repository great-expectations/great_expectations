[![Python Versions](https://img.shields.io/pypi/pyversions/great_expectations.svg)](https://pypi.python.org/pypi/great_expectations)
[![PyPI](https://img.shields.io/pypi/v/great_expectations)](https://pypi.org/project/great-expectations/#history)
[![PyPI Downloads](https://img.shields.io/pypi/dm/great-expectations)](https://pypistats.org/packages/great-expectations)
[![Build Status](https://img.shields.io/azure-devops/build/great-expectations/bedaf2c2-4c4a-4b37-87b0-3877190e71f5/1)](https://dev.azure.com/great-expectations/great_expectations/_build/latest?definitionId=1&branchName=develop)
[![pre-commit.ci Status](https://results.pre-commit.ci/badge/github/great-expectations/great_expectations/develop.svg)](https://results.pre-commit.ci/latest/github/great-expectations/great_expectations/develop)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.5683574.svg)](https://doi.org/10.5281/zenodo.5683574)
[![Twitter Follow](https://img.shields.io/twitter/follow/expectgreatdata?style=social)](https://twitter.com/expectgreatdata)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://greatexpectations.io/slack)
[![Contributors](https://img.shields.io/github/contributors/great-expectations/great_expectations)](https://github.com/great-expectations/great_expectations/graphs/contributors)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

<!-- <<<Super-quickstart links go here>>> -->

<img align="right" src="./docs/docusaurus/static/img/gx-mark-160.png">

## About GX Core

GX Core combines the collective wisdom of thousands of community members with a proven track record in data quality deployments worldwide, wrapped into a super-simple package for data teams.

Its powerful technical tools start with Expectations: expressive and extensible unit tests for your data. Expectations foster collaboration by giving teams a common language to express data quality tests in an intuitive way. You can automatically generate documentation for each set of validation results, making it easy for everyone to stay on the same page. This not only simplifies your data quality processes, but helps preserve your organization’s institutional knowledge about its data.

Learn more about how data teams are using GX Core in our featured [case studies](https://greatexpectations.io/case-studies/).

## Integration support policy

GX Core supports Python `3.10` through `3.14`.
Experimental support for Python `3.15` and later can be enabled by setting a `GX_PYTHON_EXPERIMENTAL` environment variable when installing `great_expectations`.

For data sources and other integrations that GX supports, see the [compatibility reference](https://docs.greatexpectations.io/docs/help/compatibility_reference) for additional information.

## Get started

GX recommends deploying GX Core within a virtual environment. For more information about getting started with GX Core, see [Introduction to GX Core](https://docs.greatexpectations.io/docs/core/introduction/).

1. Run the following command in an empty base directory inside a Python virtual environment to install GX Core:

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
We truly value the contributions of our community and always welcome pull requests. PRs are encouraged for both bug fixes and new features. For feature requests, we ask that you first open an issue for discussion to ensure the feature fits within the vision for GX Core and to align on the approach so that your time and effort are well spent. Thank you for being a crucial part of GX Core!

See [CONTRIBUTING.md](./CONTRIBUTING.md) for details on how to propose a change, claim an issue, and submit a pull request.

## Code of conduct
Everyone interacting in GX Core project codebases, Discourse forums, Slack channels, and email communications is expected to adhere to the [GX Community Code of Conduct](https://discourse.greatexpectations.io/t/gx-community-code-of-conduct/1199).
