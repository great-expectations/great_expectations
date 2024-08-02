import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../../_core_components/prerequisites/_gx_installation.md'

The environment variable `GX_ANALYTICS_ENABLED` can be used to toggle the collection of analytics information.  This is particularly useful when using an Ephemeral Data Context because an Ephemeral Data Context does not persist between Python sessions and therefore won't persist configuration changes made using `context.variables` in Python.

`GX_ANALYTICS_ENABLED` will also work to toggle analytics collection when using a GX Cloud Data Context or a File Data Context.

### Prerequisites

- <PrereqPythonInstalled/>
- <PrereqGxInstalled/>
- Permissions necessary to set local Environment Variables.

### Procedure

1. Set the environment variable `GX_ANALYTICS_ENABLED`.

   You can set environment variables through terminal commands or by adding the equivalent commands to your `~/.bashrc` file.  If you set the environment variable directly from the terminal, the environment variable will not persist beyond the current session.  If you add them to the `~/.bashrc` file, the variable will be exported each time you log in.

   To set the `GX_ANALYTICS_ENABLED` environment variable, use the command:

   ```bash title="Terminal or ~/.bashrc"
   export GX_ANALYTICS_ENABLED=False
   ```
   
   or:

   ```bash title="Terminal or ~/.bashrc"
   export GX_ANALYTICS_ENABLED=True
   ```

2. Initialize a Data Context in Python.

   Analytics settings are checked when a Data Context is initialized.  Therefore, for a change to `GX_ANALYTICS_ENABLED` to take effect, the Data Context must be initialized *after* the environment variable has been set.

   For more information on initializing a Data Context, see [Create a Data Context](/core/set_up_a_gx_environment/create_a_data_context.md).