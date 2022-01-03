---
title: Data Context
id: data_context
hoverText: The primary entry point for a Great Expectations deployment, with configurations and methods for all supporting components.
---

A Data Context is the primary entry point for a Great Expectations deployment, with configurations and methods for all supporting components.



## NOTES: TEMPORARY
Describes a Great Expectations project and composed of expectation suites, datasources, actions, and how they relate. All of the primary methods for configuring DataSources, ExpectationSuites, Checkpoints, and the various metadata Stores are within the DataContext, as well as methods for creating Batches, running validation, etc. The first thing you do when working with Great Expectations is initialize a DataContext.

## [Core Promises for the “Setup : Overview” doc](https://docs.google.com/document/d/1cit1B2-LhW2CafQpbbkY5FVHmynysQUnoftsJSSixkw/edit)

- The DataContext is your primary entry point to all of Great Expectations’ APIs.
  - The DataContext provides convenience methods for accessing common objects based on untyped input (e.g. get_batch) or common defaults (e.g. read_csv).
  - Internal workflows are strongly typed, but we make exceptions for a handful of convenience methods on the DataContext.
  - The DataContext also makes it easy to handle configuration of its own top-level components.
    - The DataContext includes basic CRUD operations for all of the core components of a Great Expectations deployment (DataSources, ExpectationSuites, Checkpoints)
    - The DataContext also provides access and default integrations with DataDocs, MetricStore, secret store, plugins, etc.
    - The DataContext also provides convenience methods for testing configuration (test_yaml_config)
  - You can store the configs and data that back up a Data Context in a variety of ways.
    - The Getting Started tutorial does everything locally. This is a simple way to get started.
    - For production deployments, you’ll probably want to swap out some of the components.
    - You can see several soup-to-nuts examples in the Reference Architectures section of the documentation.
    - If the exact deployment pattern you want to follow isn’t documented in a Reference Architecture, you can see details for configuring specific components in our How-to Guides.
    - The DataContext {{Where is the DataContext actually stored? What about its }}
    - Allows seamless upgrading from open source to Cloud.

{As we saw in the Getting Started,} The DataContext is your primary entry point to all of Great Expectations’ APIs.

## Instantiating a DataContext

As a Great Expectations user, you will almost always start by instantiating a DataContext:

```
import great_expectations as ge
context = ge.get_context()
```

Alternatively, you might call:

```
import great_expectations as ge
context = ge.get_context(filepath=”something”)
```

If you’re using Great Expectations cloud, you’d call:
```
import great_expectations as ge
context = ge.get_context(API_KEY=”something”)
```

For a full set of examples, please see {{these other docs.}}

That’s it! You now have access to all the goodness of a DataContext.

## Configuration

The DataContext provides your primary API for configuring all the components of all kinds of deployments for Great Expectations.

- The DataContext includes basic CRUD operations for all of the core components of a Great Expectations deployment (DataSources, ExpectationSuites, Checkpoints)
  - The DataContext also provides access and default integration with DataDocs, MetricStore, secret store, plugins, etc.
  - The DataContext also provides convenience methods for testing configuration (test_yaml_config)
    - test_yaml_config never overwrites the underlying config. You need to explicitly save the configuration first.

## Convenience methods

The DataContext provides convenience methods for accessing common objects based on untyped input and/or common defaults.

```
get_batch/get_validator
read_csv
…
?
```

These methods are covered in more detail in the linked reference documentation. These methods make it much simpler for new users of Great Expectations to get started, by removing the need to fully understand details of internal APIs.

For example, a simple call to 

```
my_df = context.read_csv(“my_data.csv”)
```

replaces all of this boilerplate:

```
#import a bunch of stuff
#configure and instantiate an ephemeral data source
#get a batch through that data source
```

## Untyped Inputs

Another way that the DataContext makes it easy to get started is by allowing untyped inputs on most methods. For example, to get a batch with typed input, you would call:

```
from great_expectations.core.batch import BatchRequest

batch_request = BatchRequest(
    datasource_name="my_azure_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
)

context.get_batch(
    batch_request=batch_request
)
```

However, we can take some of the friction out, by allowing untyped inputs:

```
context.get_batch(
    datasource_name="my_azure_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
)
```

In this example, the get_batch method takes on the responsibility for inferring your intended types, and passing it through to the correct internal methods.

This distinction around untyped inputs reflects an important architecture decision within the Great Expectations codebase: “Internal workflows are strongly typed, but we make exceptions for a handful of convenience methods on the DataContext.”

Stronger type-checking allows the building of cleaner code, with stronger guarantees and a better understanding of error states. It also allows us to take advantage of tools like static type checkers, cyclometric complexity analysis, etc.

However, requiring typed inputs creates a steep learning curve for new users. For example, the first method above can be intimidating if you haven’t done a deep dive on exactly what a `BatchRequest` is. It also requires you to know that a Batch Request is imported from `great_expectations.core.batch`.

Allowing untyped inputs makes it possible to get started much more quickly in Great Expectations. However, the risk is that untyped inputs will lead to confusion. To head off that risk, we follow the following principles:
Type inference is conservative. If inferring types would require guessing, the method will instead throw an error.
We raise informative errors, to help users zero in on alternatives.

For example:

```example 1: an example of get_batch that raises an informative error```

```example 2: an example of get_batch that raises an informative error```

## Storing Data Contexts

The DataContextStore handles storage of configuration for the DataContext itself.

{{You can instantiate DataContexts in a bunch of ways.}}
- List
  - List
  - List

{{You can store the configs for DataContexts in a bunch of ways.}}
- List
  - List
  - List

It doesn’t matter how you instantiate your DataContext, or store its configs---once you have the DataContext in memory, it will always behave in the same way.
