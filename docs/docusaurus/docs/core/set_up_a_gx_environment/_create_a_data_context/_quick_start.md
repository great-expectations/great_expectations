import Tabs from '@theme/Tabs'
import TabItem from '@theme/TabItem'

import PrereqPythonInstallation from '../../_core_components/prerequisites/_python_installation.md'
import PrereqGxInstallation from '../../_core_components/prerequisites/_gx_installation.md'

## Prerequisites

- <PrereqPythonInstallation/>
- <PrereqGxInstallation/> 

## Request an available Data Context

<Tabs 
   queryString="procedure"
   defaultValue="instructions"
   values={[
      {value: 'instructions', label: 'Instructions'},
      {value: 'sample_code', label: 'Sample code'}
   ]}
>

<TabItem value="instructions" label="Instructions">

1. Run the following code to request a Data Context:

   ```python title='Python input' name="core/set_up_a_gx_environment/_create_a_data_context/quick_start.py import great_expectations and get a context"
   ```

   If you don't specify parameters with the `get_context()` method, GX checks your project environment and returns the first Data Context using the following criteria:

   - `get_context()` instantiates and returns a GX Cloud Data Context if it finds the necessary credentials in your environment variables.
   - If a GX Cloud Data Context cannot be instantiated, `get_context()` will instantiate and return the first File Data Context it finds in the folder hierarchy of your current working directory.
   - If neither of the above options are viable, `get_context()` instantiates and returns an Ephemeral Data Context.

2. Optional. Run the following code to verify the type of Data Context you received:

   ```python title="Python input" name="core/set_up_a_gx_environment/_create_a_data_context/quick_start.py check_context_type"
   ```

   The name of the Data Context class is displayed.

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python input" name="core/set_up_a_gx_environment/_create_a_data_context/quick_start.py full example code"
```

</TabItem>

</Tabs>