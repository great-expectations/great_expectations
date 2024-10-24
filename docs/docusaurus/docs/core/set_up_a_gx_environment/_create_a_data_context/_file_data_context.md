import Tabs from '@theme/Tabs'
import TabItem from '@theme/TabItem'

import PrereqPythonInstallation from '../../_core_components/prerequisites/_python_installation.md'
import PrereqGxInstallation from '../../_core_components/prerequisites/_gx_installation.md'

## Prerequisites

- <PrereqPythonInstallation/>
- <PrereqGxInstallation/>

## Create a File Data Context

<Tabs 
   queryString="procedure"
   defaultValue="instructions"
   values={[
      {value: 'instructions', label: 'Instructions'},
      {value: 'sample_code', label: 'Sample code'}
   ]}
>

<TabItem value="instructions" label="Instructions">

1. Run the following code to request a File Data Context:

   ```python title='Python input' name="core/set_up_a_gx_environment/_create_a_data_context/file_data_context.py import great_expectations and get a context"
   ```

   When you specify `mode="file"`, the `get_context()` method instantiates and returns the first File Data Context it finds in the folder hierarchy of your current working directory.  

   If a File Data Context configuration is not found, `get_context(mode="file")` creates a new File Data Context in your current working directory and then instantiates and returns the newly created File Data Context.

   Alternatively, you can request a specific File Data Context by providing a folder path with the `project_root_dir` parameter.  If a File Data Context exists in the specified folder it will be instantiated and returned.  If a File Data Context is not found in the specified folder, a new File Data Context will be created.

   ```python title="Python input" name="docs/docusaurus/docs/core/set_up_a_gx_environment/_create_a_data_context/file_data_context.py - specific file data context"
   ```

3. Optional. Run the following code to review the File Data Context configuration:

   ```python title="Python input" name="core/set_up_a_gx_environment/_create_a_data_context/file_data_context.py review returned Data Context"
   ```
   
   The Data Context configuration, formatted as a Python dictionary, is displayed.

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python input" name="core/set_up_a_gx_environment/_create_a_data_context/file_data_context.py full example code"
```

</TabItem>

</Tabs>