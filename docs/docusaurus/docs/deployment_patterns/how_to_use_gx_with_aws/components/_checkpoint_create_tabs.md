import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import YamlTabCreate from './_checkpoint_create_tab_yaml.md'
import PythonTabCreate from './_checkpoint_create_tab_python.md'
import YamlTabTest from './_checkpoint_test_tab_yaml.md'


First we create the Checkpoint configuration:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

  <TabItem value="yaml">

  <YamlTabCreate />

  </TabItem>

  <TabItem value="python">

  <PythonTabCreate />

  </TabItem>
</Tabs>

Once we have defined our Checkpoint configuration, we can test our syntax using `context.test_yaml_config(...)`:

<Tabs
  groupId="yaml"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  ]}>

  <TabItem value="yaml">

  <YamlTabTest />

  </TabItem>

</Tabs>

Note that we get a message that the Checkpoint contains no validations. This is OK because we will pass them in at runtime, as we can see below when we call `context.run_checkpoint(...)`.
