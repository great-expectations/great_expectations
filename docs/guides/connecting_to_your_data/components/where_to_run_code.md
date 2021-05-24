import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Get an environment to run the code in this guide. Please choose an option below.

<Tabs
  defaultValue='cli'
  values={[
  {label: 'CLI', value:'cli'},
  {label: 'YAML', value:'yaml'},
  {label: 'python', value:'python'},
  ]}>
  <TabItem value="cli">

If you use the Great Expectations CLI, run this command to automatically generate a pre-configured jupyter notebook. Then you can follow along in the YAML-based workflow below:

```console
great_expectations --v3-api datasource new
```

</TabItem>
<TabItem value="yaml">

If you use Great Expectations in an environment that has filesystem access, and prefer not to use the CLI, run the code in this guide in a notebook or other python script.

See [🍏 CORE SKILL ICON DataContext can be initialized from disk](#) for details.

</TabItem>
<TabItem value="python">

If you use Great Expectations in an environment that has no filesystem (such as Databricks or AWS EMR), run the code in this guide in that system's preferred way.

See [🍏 CORE SKILL ICON DataContext is constructed in memory](#) for details.

</TabItem>

</Tabs>
