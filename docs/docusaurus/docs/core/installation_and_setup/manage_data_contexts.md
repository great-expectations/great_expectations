---
title: Manage Data Contexts
toc_min_heading_level: 2
toc_max_heading_level: 2
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

A Data Context defines the storage location for metadata, such as your configurations for Data Sources, Expectation Suites, Checkpoints, and Data Docs. It also contains your Validation Results and the metrics associated with them, and it provides access to those objects in Python, along with other helper functions for the GX Python API. 

The following are the available Data Context types:

- **File Data Context** - A persistent Data Context that stores metadata and configuration information as YAML files.

- **Ephemeral Data Context** - A temporary Data Context that stores metadata and configuration information in memory.  This Data Context will not persist beyond the current Python session.

- **GX Cloud Data Context** - A Data Context that connects to a GX Cloud Account to retrieve and store metadata and configuration information from the cloud.

## Prerequisites

- [A valid Python environment](/core/installation_and_setup/set_up_a_python_environment.mdx).
- [The GX Python library](/core/installation_and_setup/install_gx.md).

## Request a Data Context

1. Run the following code to request a Data Context:

  ```python title='Python'
  import great_expectations as gxe

  context = gxe.get_context()
  ```

  If you don't specify parameters with the `get_context()` method, GX checks your project environment and returns the first Data Context using the following criteria:

    - `get_context()` instantiates and returns a GX Cloud Data Context if it finds the necessary credentials in your environment variables.
    - If a GX Cloud Data Context cannot be instantiated, `get_context()` will instantiate and return the first File Data Context it finds in the folder hierarchy of your current working directory.
    - If neither of the above options are viable, `get_context()` instantiates and returns an Ephemeral Data Context.

2. Optional. Run the following code to verify the type of Data Context you received:

  ```python title="Python"
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

A Data Context is required in almost all Python scripts using GX 1.0. Use Python code to initialize, instantiate, and verify the contents of a Filesystem Data Context.

### Import GX

Run the following code to import the GX module:

```python title="Python"
import great_expectations as gx
```
### Determine the folder to initialize the Data Context in

Run the following code to initialize your Filesystem Data Context in an empty folder:

```python title="Python" name="docs/docusaurus/docs/oss/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_initialize_a_filesystem_data_context_in_python.py path_to_empty_folder"
```

### Create a Data Context

You provide the path for your empty folder to the GX library's `gx.get_context` method as the `project_root_dir` parameter.  Because you are providing a path to an empty folder, `gx.get_context` initializes a Filesystem Data Context in that location.

For convenience, the `gx.get_context` method instantiates and returns the newly initialized Data Context, which you can keep in a Python variable.

```python title="Python" name="docs/docusaurus/docs/oss/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_initialize_a_filesystem_data_context_in_python.py initialize_filesystem_data_context"
```

:::info What if the folder is not empty?

If the `project_root_dir` provided to the `gx.get_context` method points to a folder that does not already have a Data Context present, `gx.get_context` initializes a Filesystem Data Context in that location even if other files and folders are present.  This allows you to initialize a Filesystem Data Context in a folder that contains your Data Assets or other project related contents.

If a Data Context already exists in `project_root_dir`, the `gx.get_context` method will not re-initialize it.  Instead, `gx.get_context` instantiates and returns the existing Data Context.

:::

### Verify the Data Context content 

Run the following code to confirm the Data Context was instantiated correctly:

```python title="Python"
  print(context)
```
The Data Context configuration formatted as a Python dictionary appears.

</TabItem>

<TabItem value="ephemeral" label="In memory (Ephemeral)">

An Ephemeral Data Context is a temporary, in-memory Data Context.  They are ideal for doing data exploration and initial analysis when you do not want to save anything to an existing project, or for when you need to work in a hosted environment such as an EMR Spark Cluster.

An Ephemeral Data Context does not persist beyond the current Python session. To keep the contents of your Ephemeral Data Context for future use, see [Convert an Ephemeral Data Context to a Filesystem Data Context](#convert-the-ephemeral-data-context-into-a-filesystem-data-context).

### Import classes

To create your Data Context, you'll create a configuration that uses in-memory Metadata Stores. 

1. Run the following code to import the `DataContextConfig` and the `InMemoryStoreBackendDefaults` classes:

    ```python title="Python" name="docs/docusaurus/docs/snippets/how_to_explicitly_instantiate_an_ephemeral_data_context.py import_data_context_config_with_in_memory_store_backend"
    ```

2. Run the following code to import the `EphemeralDataContext` class:

    ```python title="Python" name="docs/docusaurus/docs/snippets/how_to_explicitly_instantiate_an_ephemeral_data_context.py import_ephemeral_data_context"
    ```

### Create the Data Context configuration

Run the following code to create a Data Context configuration that specifies the use of in-memory Metadata Stores and pasess an instance of the `InMemoryStoreBackendDefaults` class as a parameter when initializing an instance of the `DataContextConfig` class:

```python title="Python" name="docs/docusaurus/docs/snippets/how_to_explicitly_instantiate_an_ephemeral_data_context.py instantiate_data_context_config_with_in_memory_store_backend"
```

### Instantiate an Ephemeral Data Context

Run the following code to initialize the `EphemeralDataContext` class while passing in the `DataContextConfig` instance you created as the value of the `project_config` parameter.

```python title="Python" name="docs/docusaurus/docs/snippets/how_to_explicitly_instantiate_an_ephemeral_data_context.py instantiate_ephemeral_data_context"
```

:::info Saving the contents of an Ephemeral Data Context for future use

An Ephemeral Data Context is an in-memory Data Context that is not intended to persist beyond the current Python session. Run the following code to convert it to a Filesystem Data Context and save its contents for future use:

```python title="Python"
  print(context)
```

This method initializes a Filesystem Data Context in the current working directory containing the Ephemeral Data Context. For more information, see [Convert an Ephemeral Data Context to a Filesystem Data Context](#convert-the-ephemeral-data-context-into-a-filesystem-data-context).

:::


</TabItem>

<TabItem value="gx_cloud" label="Online (GX Cloud)">

With GX Cloud, Data Context configurations and metadata are managed for you. Instead of initializing a new Cloud Data Context, you [connect to your existing GX Cloud account in Python by using a Cloud Data Context](/core/installation_and_setup/manage_data_contexts.md?context-type=gx_cloud#connect-to-an-existing-data-context).

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

If you're using GX for multiple projects, you might want to use a different Data Context for each project. Instantiate a specific Filesystem Data Context so that you can switch between sets of previously defined GX configurations.

### Prerequisites

- [A Great Expectations instance](/core/installation_and_setup/install_gx.md).
- A previously initialized Filesystem Data Context.

### Import GX

```python title="Python"
import great_expectations as gx
```

### Initialize a Filesystem Data Context

Each Filesystem Data Context has a root folder in which it was initialized.  This root folder identifies the specific Filesystem Data Context to instantiate.

```python title="Python" name="docs/docusaurus/docs/oss/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_instantiate_a_specific_filesystem_data_context.py path_to_project_root"
```

### Run the `get_context(...)` method

You provide the path for your empty folder to the GX library's `get_context(...)` method as the `project_root_dir` parameter. Because you are providing a path to an empty folder, the `get_context(...)` method instantiates and return the Data Context at that location.

```python title="Python" name="docs/docusaurus/docs/oss/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_instantiate_a_specific_filesystem_data_context.py get_filesystem_data_context"
```

:::info Project root vs context root

There is a subtle distinction between the `project_root_dir` and `context_root_dir` arguments accepted by `get_context(...)`.

Your context root is the directory that contains your GX config while your project root refers to your actual working directory (and therefore contains the context root).

```bash
# The overall directory is your project root
data/
gx/ # The GX folder with your config is your context root
  great_expectations.yml
  ...
...
```

Both are functionally equivalent for purposes of working with a file-backed project. 
:::

:::info What if the folder does not contain a Data Context?

If the root directory provided to the `get_context(...)` method points to a folder that does not already have a Data Context, the `get_context(...)` method initializes a new Filesystem Data Context in that location.

The `get_context(...)` method instantiates and returns the newly initialized Data Context.

:::

### Verify the Data Context content 

Run the following code to confirm the Data Context was instantiated correctly:

```python title="Python"
  print(context)
```
The Data Context configuration formatted as a Python dictionary appears.

</TabItem>

<TabItem value="ephemeral" label="In memory (Ephemeral)">

Ephemeral Data Contexts are temporary. They don't persist from one Python session to the next, and you can't reconnect to an Ephemeral Data Context from a previous Python session.

When you [create an Ephemeral Data Context](/core/installation_and_setup/manage_data_contexts.md?context-type=ephemeral#initialize-a-new-data-context), store it in a Python variable so that you can continue to reference and use it in your current session.

</TabItem>

<TabItem value="gx_cloud" label="Online (GX Cloud)">

Use a Python script or interpreter, such as a Jupyter Notebook to interact with GX Cloud. You'll configure your GX Cloud environment variables, connect to sample data, build your first Expectation, validate data, and review the validation results through Python code.

### Prerequisites

- You have a [GX Cloud account](https://greatexpectations.io/cloud) and Admin or Editor permissions.

### Get your user access token and organization ID

You'll need your user access token and organization ID to set your environment variables. Don't commit your access tokens to your version control software.

1. In GX Cloud, click **Settings** > **Tokens**.

2. In the **User access tokens** pane, click **Create user access token**.

3. In the **Token name** field, enter a name for the token that will help you quickly identify it.

4. Click **Create**.

5. Copy and then paste the user access token into a temporary file. The token can't be retrieved after you close the dialog.

6. Click **Close**.

7. Copy the value in the **Organization ID** field into the temporary file with your user access token and then save the file. 

    GX recommends deleting the temporary file after you set the environment variables.

### Set the GX Cloud Organization ID and user access token as environment variables

Environment variables securely store your GX Cloud access credentials.

1. Save your **GX_CLOUD_ACCESS_TOKEN** and **GX_CLOUD_ORGANIZATION_ID** as environment variables by entering `export ENV_VAR_NAME=env_var_value` in the terminal or add the code to your `~/.bashrc` or `~/.zshrc` file. For example:

    ```bash title="Terminal input"
    export GX_CLOUD_ACCESS_TOKEN=<user_access_token>
    export GX_CLOUD_ORGANIZATION_ID=<organization_id>
    ```

    After you save your **GX_CLOUD_ACCESS_TOKEN** and **GX_CLOUD_ORGANIZTION_ID**, you can use Python scripts to access GX Cloud and complete other tasks. See the [GX OSS guides](/core/introduction/about_gx.md).

2. Optional. If you created a temporary file to record your user access token and Organization ID, delete it.

### Create a Data Context

Run the following Python code to create a Data Context object:

```python title="Python" name="tutorials/quickstart/quickstart.py get_context"
```
  
The Data Context detects the previously set environment variables and connects to your GX Cloud account.

</TabItem>

</Tabs>

## Export an Ephemeral Data Context to a new File Data Context

An Ephemeral Data Context is a temporary, in-memory Data Context that doesn't persist beyond the current Python session. To save the contents of an Ephemeral Data Context for future use you can convert it to a Filesystem Data Context.

### Prerequisites

- An Ephemeral Data Context

### Confirm your Data Context is Ephemeral

To confirm that you're working with an Ephemeral Data Context, run the following code:

```python title="Python" name="docs/docusaurus/docs/snippets/how_to_explicitly_instantiate_an_ephemeral_data_context.py check_data_context_is_ephemeral"
```

The example code assumes that your Data Context is stored in the variable `context`.

### Verify that a Filesystem Data Context doesn't exist

The method for converting an Ephemeral Data Context to a Filesystem Data Context initializes the new Filesystem Data Context in the current working directory of the Python process that is being executed.  If a Filesystem Data Context already exists at that location, the process will fail.

You can determine if your current working directory already has a Filesystem Data Context by looking for a `great_expectations.yml` file.  The presence of that file indicates that a Filesystem Data Context has already been initialized in the corresponding directory.

### Convert the Ephemeral Data Context into a Filesystem Data Context

Converting an Ephemeral Data Context into a Filesystem Data Context can be done with one line of code:

```python title="Python" name="docs/docusaurus/docs/snippets/how_to_explicitly_instantiate_an_ephemeral_data_context.py convert_ephemeral_data_context_filesystem_data_context"
```

:::info Replacing the Ephemeral Data Context

The `convert_to_file_context()` method does not change the Ephemeral Data Context itself.  Rather, it initializes a new Filesystem Data Context with the contents of the Ephemeral Data Context and then returns an instance of the new Filesystem Data Context.  If you do not replace the Ephemeral Data Context instance with the Filesystem Data Context instance, it will be possible for you to continue using the Ephemeral Data Context.  

If you do this, it is important to note that changes to the Ephemeral Data Context **will not be reflected** in the Filesystem Data Context.  Moreover, `convert_to_file_context()` does not support merge operations. This means you will not be able to save any additional changes you have made to the content of the Ephemeral Data Context.  Neither will you be able to use `convert_to_file_context()` to replace the Filesystem Data Context you had previously created: `convert_to_file_context()` will fail if a Filesystem Data Context already exists in the current working directory.

GX recommends that you stop using the Ephemeral Data Context instance after you convert your Ephemeral Data Context to a Filesystem Data Context.

:::

## View a Data Context configuration

Run the following code to view Data Context configuration information:

```python title="Python"
  from great_expectations.data_context import EphemeralDataContext, CloudDataContext, FileDataContext
  
  print("Cloud:", isinstance(context, CloudDataContext))
  print("File:", isinstance(context, FileDataContext))
  print("Ephemeral:", isinstance(context, EphemeralDataContext))
```

## Next steps
- [Configure credentials](/core/installation_and_setup/manage_credentials.md)
- [(Optional) Configure Stores](/core/installation_and_setup/manage_metadata_stores/manage_metadata_stores.md)
- [(Optional) Configure Data Docs](/core/installation_and_setup/manage_metadata_stores/manage_metadata_stores.md)
- [Manage and access data](/core/manage_and_access_data/manage_and_access_data.md)
- [Manage Data Sources](/core/manage_and_access_data/manage_data_sources/manage_data_sources.md)