---
title: "Setup: Overview"
---
# [![Setup Icon](../../images/universal_map/Gear-active.png)](./setup_overview.md) Setup: Overview

import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import UniversalMap from '@site/docs/images/universal_map/_universal_map.mdx';

<!--Use 'inactive' or 'active' to indicate which Universal Map steps this term has a use case within.-->

<UniversalMap setup='active' connect='inactive' create='inactive' validate='inactive'/>

<!-- Only keep one of the 'To best understand this document' lines.  For processes like the Universal Map steps, use the first one.  For processes like the Architecture Reviews, use the second one. -->

:::note Prerequisites
- Completing the [Quickstart guide](tutorials/quickstart/quickstart.md) is recommended.
:::

Getting started with Great Expectations is quick and easy.  Once you have completed setup for your production deployment, you will have access to all the features of Great Expectations from a single entry point: Your <TechnicalTag relative="../" tag="data_context" text="Data Context" />.  You will also have your <TechnicalTag relative="../" tag="store" text="Stores" /> and <TechnicalTag relative="../" tag="data_docs" text="Data Docs" /> configured in the manner most suitable for your project's purposes.

### The alternative to manual Setup

If you're not interested in managing your own configuration or infrastructure then Great Expectations Cloud may be of interest to you. You can learn more about Great Expectations Cloud — our fully managed SaaS offering — by signing up for [our weekly cloud workshop!](https://greatexpectations.io/cloud) You’ll get to see our newest features and apply for our private Alpha program!

## The Setup process

<!-- Brief outline of what the process entails.  -->

Note: configuration of <TechnicalTag relative="../" tag="datasource" text="Datasources" />, <TechnicalTag relative="../" tag="expectation_suite" text="Expectation Suites" />, and <TechnicalTag relative="../" tag="checkpoint" text="Checkpoints" /> will be handled separately after your main Great Expectations deployment is set up.

<!-- The following subsections should be repeated as necessary.  They should give a high level map of the things that need to be done or optionally can be done in this process, preferably in the order that they should be addressed (assuming there is one). If the process crosses multiple steps of the Universal Map, use the <SetupHeader> <ConnectHeader> <CreateHeader> and <ValidateHeader> tags to indicate which Universal Map step the subsections fall under. -->

### 1. System Dependencies

- Python (version 3.7 or greater) and `pip`

We recommend using a virtual environment for your project's workspace.

For help, see our <TechnicalTag relative="../" tag="supporting_resource" text="Supporting Resources" /> which include more information and tutorials.

### 2. Installation

```markup title="Terminal command:"
pip install great_expectations
```

See our [installation guide](./index.md#installation) for more information.

### 3. Initialize a Data Context

Your Data Context contains the entirety of your Great Expectations project and provides the entry point for all of the primary methods you will use to configure and interact with Great Expectations. 

![what the data context does for you](../images/overview_illustrations/data_context_does_for_you.png)

```markdown title="Terminal command:"
great_expectations init
```

Running this command will initialize your Data Context in the directory that the command is run from.  It will create the folder structure a Data Context requires to organize your project.

See our [Data Context configuration guide](./index.md#data-contexts) for more information.

### 4. Optional Configurations

Once your Data Context is initialized, you'll be all set to start using Great Expectations.  However, there are a few things that are configured by default to operate locally which you may want to configure to be hosted elsewhere. Using the Data Context, you can easily create and test your configurations.

#### Stores

Stores are the locations where your Data Context stores information about your <TechnicalTag relative="../" tag="expectation" text="Expectations" />, your <TechnicalTag relative="../" tag="validation_result" text="Validation Results" />, and your <TechnicalTag relative="../" tag="metric" text="Metrics" />.  By default, these are stored locally, but you can configure them to work with a variety of backends.

See our [Stores configuration guide](./index.md#stores) for more information.

#### Data Docs

Data Docs provide human readable renderings of your Expectation Suites and Validation Results.  As with Stores, these are built locally by default.  However, you can configure them to be hosted and shared in a variety of different ways.

See our [Data Docs configuration guide](./index.md#data-docs) for more information.

#### Plugins

Python files are treated as <TechnicalTag relative="../" tag="plugin" text="Plugins" /> if they are in the `plugins` directory in your project (which is created automatically when you initialize your Data Context). They can be used to extend Great Expectations.  If you have <TechnicalTag relative="../" tag="custom_expectation" text="Custom Expectations" /> or other extensions to Great Expectations that you wish to use as Plugins in your deployment of Great Expectations, you should include them in the `plugins` directory.

## Wrapping up

The Data Context (as illustrated below) is the canonical starting point for everything else you do through Great Expectations.

```markdown title="Python code:"
import great_expectations as gx
context = gx.get_context()
```
