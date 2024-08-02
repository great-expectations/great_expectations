import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../../_core_components/prerequisites/_gx_installation.md';
import PrereqFileDataContext from '../../_core_components/prerequisites/_file_data_context.md';

The Data Context variable `analytics_enabled` can be used to toggle the collection of analytics information.  Because the analytics configuration is loaded when a Data Context is initialized this method is only suitable when working with a File Data Context.  For other types of Data Context, use the [Environment Variable](./toggle_analytics_events.md?config_method=environment_variable#methods-for-toggling-analytics-collection) method for toggling analytics collection.


### Prerequisites

- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.
- <PrereqFileDataContext/>.

### Procedure

1. Set the Data Context variable `analytics_enabled`.

   You can set environment variables through terminal commands or by adding the equivalent commands to your `~/.bashrc` file.  If you set the environment variable directly from the terminal, the environment variable will not persist beyond the current session.  If you add them to the `~/.bashrc` file, the variable will be exported each time you log in.

   To set the `analytics_enabled` Data Context variable, use the command:

   ```python title="Python"
   context.variables.analytics_enabled = False
   ```

   or:

   ```python title="Python"
   context.variables.analytics_enabled = True
   ```

2. Save changes to `analytics_enabled`.

   Changes to your File Data Context's variables reside in memory and will not persist between sessions unless they are explicitly pushed to the Data Context's configuration file. To push your changes to the Data Context's configuration file execute the following code:

   ```python title="Python"
   context.variables.save_config()
   ```

4. Re-initialize the Data Context.

   Analytics settings are checked when a Data Context is initialized.  Therefore, for a change to `analytics_enabled` to take effect, the File Data Context must be re-initialized after the change has been saved to the Data Context's configuration file.

   ```python title="Python"
   context = gx.get_context(mode="file")
   ```

   For more information on initializing a Data Context, see [Create a Data Context](/core/set_up_a_gx_environment/create_a_data_context.md).