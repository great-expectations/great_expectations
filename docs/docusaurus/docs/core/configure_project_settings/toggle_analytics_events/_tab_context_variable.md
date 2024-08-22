import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../../_core_components/prerequisites/_gx_installation.md';
import PrereqFileDataContext from '../../_core_components/prerequisites/_file_data_context.md';

The Data Context variable `analytics_enabled` can be used to toggle the collection of analytics information.  Because the analytics configuration is loaded when a Data Context is initialized this method is only suitable when working with a File Data Context.  For other types of Data Context, use the [Environment Variable](/core/configure_project_settings/toggle_analytics_events/toggle_analytics_events.md?config_method=environment_variable#methods-for-toggling-analytics-collection) method for toggling analytics collection.


### Prerequisites

- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.
- <PrereqFileDataContext/>.

### Procedure

<Tabs>

<TabItem value="tutorial" label="Tutorial">

1. Set the Data Context variable `analytics_enabled`.

   With a File Data Context you can set the `analytics_enabled` Data Context variable and your setting will persist between sessions.

   To set the `analytics_enabled` Data Context variable, use the command:

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/_examples/toggle_analytics_events.py - disable Analytics Events"
   ```

   or:

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/_examples/toggle_analytics_events.py - enable Analytics Events"
   ```

2. Save changes to `analytics_enabled`.

   Changes to your File Data Context's variables reside in memory and will not persist between sessions unless they are explicitly pushed to the Data Context's configuration file. To push your changes to the Data Context's configuration file execute the following code:

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/_examples/toggle_analytics_events.py - save changes to the Data Context"
   ```

4. Re-initialize the Data Context.

   Analytics settings are checked when a Data Context is initialized.  Therefore, for a change to `analytics_enabled` to take effect, the File Data Context must be re-initialized after the change has been saved to the Data Context's configuration file.

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/_examples/toggle_analytics_events.py - re-initialize the Data Context"
   ```

   For more information on initializing a Data Context, see [Create a Data Context](/core/set_up_a_gx_environment/create_a_data_context.md).

</TabItem>

<TabItem value="sample_code" label="Sample code">

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/_examples/toggle_analytics_events.py - full code example"
   ```

</TabItem>

</Tabs>