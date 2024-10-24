import Tabs from '@theme/Tabs'
import TabItem from '@theme/TabItem'

import PrereqPythonInstallation from '../../_core_components/prerequisites/_python_installation.md'
import PrereqGxInstallation from '../../_core_components/prerequisites/_gx_installation.md'

## Prerequisites

- <PrereqPythonInstallation/>
- <PrereqGxInstallation/>
- A GX Cloud access token and organization ID set as environment variables. (See **Configure credentials** under [Create a Cloud Data Context](#create-a-cloud-data-context).)

## Create a Cloud Data Context

<Tabs 
   queryString="procedure"
   defaultValue="instructions"
   values={[
      {value: 'instructions', label: 'Instructions'},
      {value: 'sample_code', label: 'Sample code'}
   ]}
>

<TabItem value="instructions" label="Instructions">

1. Run the following code to request a GX Cloud Data Context:

   ```python title='Python input' name="core/set_up_a_gx_environment/_create_a_data_context/cloud_data_context.py import great_expectations and get a context"
   ```

   When you specify `mode="cloud"`, the `get_context()` method uses the **GX_CLOUD_ACCESS_TOKEN** and **GX_CLOUD_ORGANIZATION_ID** environment variables to connect to your GX Cloud account.

2. Optional. Run the following code to review the Cloud Data Context configuration:

   ```python title="Python input" name="core/set_up_a_gx_environment/_create_a_data_context/cloud_data_context.py review returned Data Context"
   ```
   
   The Data Context configuration, formatted as a Python dictionary, is displayed.

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python code" name="core/set_up_a_gx_environment/_create_a_data_context/cloud_data_context.py full example code"
```

</TabItem>

<TabItem value="cloud_credentials" label="Configure credentials">

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

1. Save your **GX_CLOUD_ACCESS_TOKEN** and **GX_CLOUD_ORGANIZATION_ID** as environment variables by entering `export ENV_VAR_NAME=env_var_value` in the terminal or adding the command to your `~/.bashrc` or `~/.zshrc` file. For example:

   ```bash title="Terminal input"
   export GX_CLOUD_ACCESS_TOKEN=<user_access_token>
   export GX_CLOUD_ORGANIZATION_ID=<organization_id>
   ```

2. Optional. If you created a temporary file to record your user access token and Organization ID, delete it.

</TabItem>

</Tabs>

