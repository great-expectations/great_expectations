---
sidebar_label: 'GX installation and configuration workflow'
title: "Great Expectations installation and configuration workflow"
---
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import UniversalMap from '@site/docs/images/universal_map/_universal_map.mdx';
import GxData from '/docs/components/_data.jsx';

<!--Use 'inactive' or 'active' to indicate which Universal Map steps this term has a use case within.-->

Setting up Great Expectations (GX) includes installing GX and initializing your deployment. Optionally, you can customize the configuration of some components, such as Stores, Data Docs, and Plugins.

After you've completed the setup for your production deployment, you can access all GX features from your <TechnicalTag relative="../" tag="data_context" text="Data Context" />. Also, your <TechnicalTag relative="../" tag="store" text="Stores" /> and <TechnicalTag relative="../" tag="data_docs" text="Data Docs" /> will be optimized for your business requirements.

To set up <TechnicalTag relative="../" tag="datasource" text="Datasources" />, <TechnicalTag relative="../" tag="expectation_suite" text="Expectation Suites" />, and <TechnicalTag relative="../" tag="checkpoint" text="Checkpoints" /> see the specific topics for these components. 

If you don't want to manage your own configurations and infrastructure, then Great Expectations Cloud might be the solution. If you're interested in participating in the Great Expectations Cloud Beta program, or you want to receive progress updates, [**sign up for the Beta program**](https://greatexpectations.io/cloud).

:::info Windows Support

Windows support for the open source Python version of GX is currently unavailable. If you’re using GX in a Windows environment, you might experience errors or performance issues.

:::

## Before you start

Before you start installing and configuring GX, you should complete the [Quickstart guide](tutorials/quickstart/quickstart.md) and have the following items installed:

- <span>A supported version of Python. GX supports Python versions {GxData.min_python} to {GxData.max_python}.</span>
- pip (the package installer for Python).
- An internet connection.
- A web browser (for Jupyter Notebooks).
- A virtual environment. Recommended for your project workspace.

## Install Great Expectations

See [Install Great Expectations](./installation/install_gx.md).

## Initialize a Data Context

Your Data Context contains your Great Expectations project, and it is the entry point for configuring and interacting with Great Expectations. The Data Context manages various classes and helps limit the number of objects you need to manage to get Great Expectations working.

![what the data context does for you](../images/overview_illustrations/data_context_does_for_you.png)

See [Configure Data Contexts](./configure_data_contexts_lp.md).

## Optional configurations

After you've initialized your Data Context, you can start using Great Expectations. However, a few components such as Stores, Data Docs, and Plugins that are configured by default to operate locally can be changed to hosted if it better suits your use case.

### Stores

Stores are the locations where your Data Context stores information about your <TechnicalTag relative="../" tag="expectation" text="Expectations" />, your <TechnicalTag relative="../" tag="validation_result" text="Validation Results" />, and your <TechnicalTag relative="../" tag="metric" text="Metrics" />.  By default, these are stored locally. To reconfigure a Store to work with a specific backend, see [Configure Expectation Stores](./configuring_metadata_stores/configure_expectation_stores.md), [Configure Validation Result Stores](./configuring_metadata_stores/configure_result_stores.md), and [Configure a MetricStore](./configuring_metadata_stores/how_to_configure_a_metricsstore.md).

### Data Docs

Data Docs provide human-readable renderings of your Expectation Suites and Validation Results, and they are built locally by default. To host and share Data Docs differently, see [Host and share Data Docs](./configuring_data_docs/host_and_share_data_docs.md).

### Plugins

Python files are treated as <TechnicalTag relative="../" tag="plugin" text="Plugins" /> when they are in the `plugins` directory of your project (which is created automatically when you initialize your Data Context) and they can be used to extend Great Expectations.  If you have <TechnicalTag relative="../" tag="custom_expectation" text="Custom Expectations" /> or other extensions that you want to use as Plugins with Great Expectations, add them to the `plugins` directory.

## Next steps

- [Install Great Expectations](./installation/install_gx.md)
