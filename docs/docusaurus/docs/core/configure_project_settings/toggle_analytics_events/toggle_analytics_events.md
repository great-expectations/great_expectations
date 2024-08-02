import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import TabEnvironmentVaribale from './_tab_environment_variable.md';
import TabContextVariable from './_tab_context_variable.md';

In order to determine if analytics should be enabled, GX 1.0 checks two sources:

- The environment variable `GX_ANALYTICS_ENABLED`
- The Data Context variable `analytics_enabled`

If either variable is set to False, analytics collection will be disabled.  If the variables are not explicitly set to False, they are assumed to be True.

## Methods for toggling analytics collection

Select one of the following methods for toggling analytics collection:

<Tabs queryString="config_method" groupId="config_method" defaultValue='environment_variable'>

   <TabItem value="environment_variable" label="Environment Variable">
   
   <TabEnvironmentVaribale/>
   
   </TabItem>

   <TabItem value="context_variable" label="Data Context Variable">

   <TabContextVariable/>

   </TabItem>

</Tabs>