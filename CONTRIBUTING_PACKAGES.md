# Contribute packages

To submit a custom Great Expectations package, you complete the following tasks:

- [Contact the Great Expectations Developer Relations team](#contact-the-great-expectations-developer-relations-team)

- [Install the Great Expectations CLI tool](#install-the-great-expectations-cli-tool)

- [Initialize a project](#initialize-a-project)

- [Contribute to your package](#contribute-to-your-package)

- [Publish your package](#publish-your-package)

To request a documentation change, or a change that doesn't require local testing, see the [README](https://github.com/great-expectations/great_expectations/blob/develop/docs/README.md) in the `docs` repository.

To create and submit a Custom Expectation to Great Expectations for consideration, see [CONTRIBUTING_EXPECTATIONS](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_EXPECTATIONS.md) in the `great_expectations` repository.

To submit a code change to Great Expectations for consideration, see [CONTRIBUTING_CODE](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_CODE.md) in the `great_expectations` repository.

## Prerequisites

- Great Expectations installed and configured for your environment. See [Great Expectations Quickstart](https://docs.greatexpectations.io/docs/tutorials/quickstart/).

- A Custom Expectation. See [Creating Custom Expectations](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/overview/).

- A GitHub account.

- A working version of Git on your computer. See [Getting Started - Installing Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git).

- A new SSH (Secure Shell Protocol) key. See [Generating a new SSH key and adding it to the ssh-agent](https://help.github.com/en/github/authenticating-to-github/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent).

- The latest Python version installed and configured. See [Python downloads](https://www.python.org/downloads/).

- A PyPI account. See [Create an account on PyPI](https://pypi.org/account/register/).

## Contact the Great Expectations Developer Relations team

Before you develop your custom Great Expectations package, notify the Great Expectations Developer Relations team of your plans in the [Great Expectations #integrations Slack channel](https://greatexpectationstalk.slack.com/archives/C037YCYNF1Q). A member of the team will discuss your custom Great Expectations package, support your development, and help you navigate the publication and maintenance process.

## Install the Great Expectations CLI tool

Great Expectations provides the `great_expectations_contrib` command-line interface (CLI) tool to simplify the package contribution process and help you adhere to code base best practices.

The `great_expectations_contrib` CLI tool performs the following functions:

- Initializes your package structure

- Performs a series of checks to validate your package

- Publishes your package to PyPi

While you're developing your package, you must use this tool to ensure that it meets the necessary requirements.

1. Run the following command to cd to the root of the `great_expectations` codebase:

    ```bash
    cd contrib/cli
    ```

2. Run the following command to install the CLI tool:

    ```bash
    pip install -e .
    ```

3. Run the following command to verify your installation:

    ```bash
    great_expectations_contrib
    ```

## Initialize a project

After you've enabled the `great_expectations_contrib` CLI tool, you need to initialize an empty package.

1. Run the following command to initialize an empty package:

    ```bash
    great_expectations_contrib init
    ```

2. Enter the name of your package, the purpose of your package, and your GitHub and PyPI usernames.

3. Run the following command to access your configured package:

    ```bash
    cd <PACKAGE_NAME>
    tree
    ```
    The command returns a file structure similar to the following example:

    ```bash
    .
    ├── LICENSE
    ├── README.md
    ├── assets
    ├── package_info.yml
    ├── requirements.txt
    ├── setup.py
    ├── tests
    │   ├── __init__.py
    │   ├── expectations
    │   │   └── __init__.py
    │   ├── metrics
    │   │   └── __init__.py
    │   └── profilers
    │       └── __init__.py
    └── <YOUR_PACKAGE_SOURCE_CODE>
        ├── __init__.py
        ├── expectations
        │   └── __init__.py
        ├── metrics
        │   └── __init__.py
        └── profilers
            └── __init__.py
    ```
    To ensure consistency with other packages, maintain this general structure as you develop you package.

## Contribute to your package

1. Record any dependencies in your `requirements.txt` file, validate your code
in `tests`, describe your package capabilities in `README.md`, and update publishing details in `setup.py`.

2. Optional. Update package metadata and assign code owners or domain experts. See
`package_info.yml`.

3. Run the following command to run checks on your package including code formatting, annotated function signature availability, and Expectation documentation:

    ```bash
    great_expectations_contrib check
    ```

## Publish your package

Publish your package after you have tested and verified its behavior and documented its capabilities.

1. Run the following command to publish your package:

    ```bash
    great_expectations_contrib publish
    ```
2. Enter your PyPI username and password.

## Issue tags

Great Expectations uses a `stalebot` to automatically tag issues without activity as `stale`, and closes them when a response is not received within a week. To prevent `stalebot` from closing an issue, you can add the `stalebot-exempt` tag.

Additionally, Great Expectations adds the following tags to indicate issue status:

- The`help wanted` tag identifies useful issues that require help from community contributors to accelerate development.

- The `enhacement` and `expectation-request` tags identify new Great Expectations features that require additional investigation and discussion.

- The `good first issue` tag identifies issues that provide an introduction to the Great Expectations contribution process.
