import Tabs from '@theme/Tabs'
import TabItem from '@theme/TabItem'

import PrereqPythonInstallation from '../../_core_components/prerequisites/_python_installation.md'
import PrereqGxInstallation from '../../_core_components/prerequisites/_gx_installation.md'

## Prerequisites

- <PrereqPythonInstallation/>
- <PrereqGxInstallation/>

## Create an Ephemeral Data Context

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Run the following code to request a File Data Context:

   ```python title='Python input' name="core/set_up_a_gx_environment/_create_a_data_context/ephemeral_data_context.py import great_expectations and get a context"
   ```

   Because Ephemeral Data Contexts are intended to be temporary, `get_context(mode='ephemeral')` will *always* instantiate and return a new Ephemeral Data Context.

2. Optional. Run the following code to review the configuration of the Ephemeral Data Context you recieved:

   ```python title="Python input" name="core/set_up_a_gx_environment/_create_a_data_context/ephemeral_data_context.py review returned Data Context"
   ```
   
   The Data Context configuration, formatted as a Python dictionary, is displayed.

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python input" name="core/set_up_a_gx_environment/_create_a_data_context/ephemeral_data_context.py full example code"
```

</TabItem>

</Tabs>