---
title: Installing in great_expectations in Local Python3
---
import Prerequisites from '/docs/guides/setup/components/install_prereq.jsx'
import NextSteps from '/docs/guides/setup/components/install_nextsteps.md'
import Congratulations from '/docs/guides/setup/components/install_congrats.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you Install Great Expectations locally for use with python

<Prerequisites>

</Prerequisites>

## Steps

### 1. Install required dependencies

First, check that you have a python3 with pip installed

```console
python --version
# or if multiple versions of python installed
python3 --version
python3 -m pip --version
```

### 2. Install Great Expectations
```console
pip install great_expectations
great_expectations --v3-api init
Using v3 (Batch Request) API

  ___              _     ___                  _        _   _
 / __|_ _ ___ __ _| |_  | __|_ ___ __  ___ __| |_ __ _| |_(_)___ _ _  ___
| (_ | '_/ -_) _` |  _| | _|\ \ / '_ \/ -_) _|  _/ _` |  _| / _ \ ' \(_-<
 \___|_| \___\__,_|\__| |___/_\_\ .__/\___\__|\__\__,_|\__|_\___/_||_/__/
                                |_|
             ~ Always know what to expect from your data ~

Let's create a new Data Context to hold your project configuration.

Great Expectations will create a new directory with the following structure:

    great_expectations
    |-- great_expectations.yml
    |-- expectations
    |-- checkpoints
    |-- notebooks
    |-- plugins
    |-- .gitignore
    |-- uncommitted
        |-- config_variables.yml
        |-- data_docs
        |-- validations

OK to proceed? [Y/n]: Y

================================================================================

Congratulations! You are now ready to customize your Great Expectations configuration.

You can customize your configuration in many ways. Here are some examples:

  Use the CLI to:
    - Run `great_expectations --v3-api datasource new` to connect to your data.
    - Run `great_expectations --v3-api checkpoint new <checkpoint_name>` to bundle data with Expectation Suite(s) in a Checkpoint for later re-validation.
    - Run `great_expectations --v3-api suite --help` to create, edit, list, profile Expectation Suites.
    - Run `great_expectations --v3-api docs --help` to build and manage Data Docs sites.

  Edit your configuration in great_expectations.yml to:
    - Move Stores to the cloud
    - Add Slack notifications, PagerDuty alerts, etc.
    - Customize your Data Docs

Please see our documentation for more configuration options!

```

<Congratulations />

## Additional Notes

To view the full scripts used in this page, see them on GitHub:

## Next Steps

<NextSteps />
