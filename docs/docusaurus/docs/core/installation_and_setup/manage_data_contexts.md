---
title: Manage Data Contexts
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import InProgress from '../_core_components/_in_progress.md'

A Data Context is your entry point to Great Expectations (GX).  It tells GX where to store metadata such as your configurations for Data Sources, Expectation Suites, Checkpoints, and Data Docs.  It contains your Validation Results and the metrics associated with them.  The Data Context also provides access to those objects in Python, along with other helper functions for the GX Python API. 

There are three types of Data Context:

- File Data Context: A persistent Data Context that stores metadata and configuration information as YAML files.
- Ephemeral Data Context: A temporary Data Context that stores metadata and configuration information in memory.  This Data Context will not persist beyond the current Python session.
- GX Cloud Data Context: A Data Context that connects to a GX Cloud Account to retrieve and store metadata and configuration information from the cloud.

You will almost always instantiate a Data Context as the first step when using GX in a Python environment.

## Prerequisites

- [A valid Python environment](/core/installation_and_setup/set_up_a_python_environment.mdx).
- [The Great Expectations Python library installed](/core/installation_and_setup/install_gx.md).

## Quickstart with a Data Context

1. You can immediately request a Data Context by running:

  ```python title='Python code'
  import great_expectations as gxe

  context = gxe.get_context()
  ```

  When no parameters are provided to the `get_context()` method, GX will attempt to determine the best Data Context to provide.  GX will check your project environment and return the first Data Context it can from the following:

    - `get_context()` will instantiate and return a GX Cloud Data Context if it finds the necessary credentials in your environment variables.
    - If a GX Cloud Data Context cannot be instantiated, `get_context()` will instantiate and return the first File Data Context it finds in the folder hierarchy of your current working directory.
    - If neither of the above options are viable, `get_context()` will instantiate and return an Ephemeral Data Context.

2. (Optional) Verify the type of Data Context you received by running:

  ```python title="Python code"
  from great_expectations.data_context import EphemeralDataContext, CloudDataContext, FileDataContext
  
  print("Cloud:", isinstance(context, CloudDataContext))
  print("File:", isinstance(context, FileDataContext))
  print("Ephemeral:", isinstance(context, EphemeralDataContext))
  ```

## Initialize a new Data Context

<Tabs 
  queryString="context-type" 
  groupId="context-type" 
  defaultValue="file" 
  values={[
   {label: 'Local (File)', value:'file'},
   {label: 'In memory (Ephemeral)', value:'ephemeral'},
   {label: 'Online (GX Cloud)', value: 'gx_cloud'}
  ]}>

  <TabItem value="file" label="Local (File)">
<InProgress/>
  </TabItem>

  <TabItem value="ephemeral" label="In memory (Ephemeral)">
<InProgress/>
  </TabItem>

  <TabItem value="gx_cloud" label="Online (GX Cloud)">

  With GX Cloud all the configurations and metadata associated with a Data Context are managed for you by the GX Cloud app.  Rather than "initializing a new Cloud Data Context," you will [sign up for a GX Cloud account](https://greatexpectations.io/cloud).  From that point forward you will be able to [connect to your existing GX Cloud account in Python by using a Cloud Data Context](/core/installation_and_setup/manage_data_contexts.md?context-type=gx_cloud#connect-to-an-existing-data-context).

  </TabItem>

</Tabs>

## Connect to an existing Data Context

<Tabs
  queryString="context-type"
  groupId="context-type"
  defaultValue='file'
  values={[
   {label: 'Local (File)', value:'file'},
   {label: 'In memory (Ephemeral)', value:'ephemeral'},
   {label: 'Online (GX Cloud)', value: 'gx_cloud'}
  ]}>

  <TabItem value="file" label="Local (File)">
<InProgress/>
  </TabItem>

  <TabItem value="ephemeral" label="In memory (Ephemeral)">

Ephemeral Data Contexts are temporary.  They will not persist from one Python session to the next, and thus you cannot reconnect to an Ephemeral Data Context from a previous Python session.

When you [create an Ephemeral Data Context](/core/installation_and_setup/manage_data_contexts.md?context-type=ephemeral#initialize-a-new-data-context), you should make certain that you store the Ephemeral Data Context instance in a Python variable so that you can continue to reference and use it throughout your current session.

  </TabItem>

  <TabItem value="gx_cloud" label="Online (GX Cloud)">
<InProgress/>
  </TabItem>

</Tabs>

## Export an Ephemeral Data Context to a new File Data Context

  <InProgress/>

## View the full configuration of a data context

  <InProgress/>

## Next steps
- [Configure credentials](/core/installation_and_setup/manage_credentials.md)
- [(Optional) Configure Stores](/core/installation_and_setup//manage_metadata_stores.md)
- [(Optional) Configure Data Docs](/core/installation_and_setup//manage_metadata_stores.md)
- [Manage and access data](/core/manage_and_access_data/manage_and_access_data.md)