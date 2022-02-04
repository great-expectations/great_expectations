import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Get an environment to run the code in this guide. Please choose an option below.

<Tabs
  groupId="yaml-or-python"
  defaultValue='cli'
  values={[
  {label: 'CLI + filesystem', value:'cli'},
  {label: 'No CLI + filesystem', value:'yaml'},
  {label: 'No CLI + no filesystem', value:'python'},
  ]}>
  <TabItem value="cli">

If you use the Great Expectations CLI, run this command to automatically generate a pre-configured Jupyter Notebook. Then you can follow along in the YAML-based workflow below:

```console
great_expectations datasource new
```

</TabItem>
<TabItem value="yaml">

If you use Great Expectations in an environment that has filesystem access, and prefer not to use the CLI, run the code in this guide in a notebook or other Python script.

</TabItem>
<TabItem value="python">

If you use Great Expectations in an environment that has no filesystem (such as Databricks or AWS EMR), run the code in this guide in that system's preferred way.

</TabItem>

</Tabs>
