---
title: Data Context
id: data_context
hoverText: The primary entry point for a GX deployment, with configurations and methods for all supporting components.
---

import TechnicalTag from '../term_tags/_tag.mdx';

A Data Context is the primary entry point for a Great Expectations (GX) deployment, and it provides the configurations and methods for all supporting GX components.

As the primary entry point for the GX API, the Data Context provides a convenient method for accessing common objects based on untyped input or common defaults. A Data Context also allows you to configure top-level components, and you can use different storage methodologies to back up your Data Context configuration. After you instantiate your `DataContext` and store its configurations, it always behaves the same way.

## Relationships to other objects

Your Data Context provides you with the methods to configure your Stores, plugins, and Data Docs.  It also provides the methods needed to create, configure, and access your <TechnicalTag relative="../" tag="datasource" text="Data Sources" />, <TechnicalTag relative="../" tag="expectation" text="Expectations" />, and <TechnicalTag relative="../" tag="checkpoint" text="Checkpoints" />.  In addition, a Data Context helps you manage your <TechnicalTag relative="../" tag="metric" text="Metrics" />, <TechnicalTag relative="../" tag="validation_result" text="Validation Results" />, and the contents of your <TechnicalTag relative="../" tag="data_docs" text="Data Docs" /> .

## Use Cases

![What your Data Context does for you throughout using GX](/docs/oss/guides/images/overview_illustrations/data_context_does_for_you.png)

When you configure your GX environment, you'll instantiate a Data Context. See [Instantiate a Data Context](/oss/guides/setup/configuring_data_contexts/instantiating_data_contexts/instantiate_data_context.md).

You can also use the Data Context to manage optional configurations for your Stores, Plugins, and Data Docs.  To configure Stores, see [Configure your GX environment](/oss/guides/setup/setup_overview_lp.md). To host and share Data Docs, see [Host and share Data Docs](/oss/guides/setup/configuring_data_docs/host_and_share_data_docs.md).

When you connect to data, you use your Data Context to create and configure Data Sources.  For more information on how to create and configure Data Sources, see [Connect to a Data Source](/oss/guides/connecting_to_your_data/connect_to_data_lp.md).

When creating Expectations, you'll use your Data Context to create <TechnicalTag relative="../" tag="expectation_suite" text="Expectation Suites" /> and Expectations, and then save them to an <TechnicalTag relative="../" tag="expectation_store" text="Expectations Store" />. The Data Context also manages Metrics and Validation Results. The Data Context manages the content of your Data Docs (displaying such things as the Validation Results and Expectations).  For more information about creating Expectations, see [Create Expectations](/oss/guides/expectations/expectations_lp.md). 

When Validating data, the Data Context provides your entry point for creating, configuring, saving, and accessing Checkpoints.  For more information on using your Data Context to create a Checkpoint, see [Validate Data](/oss/guides/validation/validate_data_lp.md). 

## Access to APIs

The Data Context provides a primary entry point to the GX API.  Your Data Context provides a convenient method for accessing common objects.  While internal workflows of GX are strongly typed, the convenience methods available from the Data Context are exceptions, allowing access based on untyped input or common defaults.

### Configuration management

A Data Context includes basic create, read, update, and delete (CRUD) operations for the core components of a GX deployment. This includes Data Sources, Expectation Suites, and Checkpoints. In addition, a Data Context allows you to access and integrate Data Docs, Stores, Plugins, and so on.

### Component management and config storage

The Data Context helps you create components such as Data Sources, Checkpoints, and Expectation Suites and manage where the information about those components is stored.  

For production deployments, you will want to define these components according to your Data Source and production environment. This may include storing information about those components in something other than your local environment. To view implementation examples for specific environments and Data Sources, see [Integrations](https://docs.greatexpectations.io/docs/0.18/oss/guides/connecting_to_your_data/connect_to_data_lp).

## GX Cloud compatibility

Because your Data Context contains the entirety of your GX project, GX Cloud can reference it to permit seamless upgrading from open source GX to GX Cloud.

## Instantiating a Data Context

After you've created a Data Context, you'll likely start future work by instantiating a `DataContext` in Python. For example:

```python title="Import GX" name="docs/docusaurus/docs/snippets/pandas_yaml_example.py import gx"
```

```python title="Python" name="docs/docusaurus/docs/snippets/pandas_yaml_example.py get_context"
```

Alternatively, you can use the `context_root_dir` parameter if you want to specify a specific directory

If you’re using GX Cloud, you set up the cloud environment variables before calling `get_context`.

## Untyped inputs

The code standards for GX strive for strongly typed inputs.  However, the Data Context's convenience functions are a noted exception to this standard.  For example, to get a Batch with typed input, you run the following code:


```python title="Python" name="docs/docusaurus/docs/snippets/pandas_yaml_example.py import BatchRequest"
```

```python title="Python" name="docs/docusaurus/docs/snippets/pandas_yaml_example.py context.get_batch with batch request"
```

If you prefer untyped inputs, you run code similar to the following examples:

```python title="Python" name="docs/docusaurus/docs/snippets/pandas_yaml_example.py context.get_batch with parameters data_asset_name"
```

```python title="Python" name="docs/docusaurus/docs/snippets/pandas_yaml_example.py context.get_batch with parameters"
```

In the example code, the `get_batch()` method is responsible for inferring your intended types, and passing it through to the correct internal methods.

This distinction between untyped and typed inputs reflects an important architectural decision within the GX codebase. Internal workflows are strongly typed, but  exceptions are allowed for a handful of convenience methods on the `DataContext`.

Stronger type-checking allows the building of cleaner code, with stronger guarantees and a better understanding of error states. It also allows GX to take advantage of tools such as static type checkers, cyclometric complexity analysis, and so on.

Requiring typed inputs can make getting started with GX challenging. For example, the first method shown in the previous code examples can be intimidating if you don't know what a `BatchRequest` is. It also requires you to know that a Batch Request is imported from `great_expectations.core.batch`. Allowing untyped inputs makes it possible to get started with GX much more quickly. However, there is always a risk is that untyped inputs will lead to confusion. To reduce or eliminate the risk, GX uses these guidleines:

- Type inference is conservative. If inferring types require guessing, the method returns an error.

- Informative errors are provided to help you determine an alternative input that does not require guessing to infer.
