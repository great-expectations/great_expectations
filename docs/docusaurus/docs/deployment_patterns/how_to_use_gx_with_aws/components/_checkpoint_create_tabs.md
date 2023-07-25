import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import YamlTabCreate from './_checkpoint_create_tab_yaml.md'
import PythonTabCreate from './_checkpoint_create_tab_python.md'
import YamlTabTest from './_checkpoint_test_tab_yaml.md'


Run the following command to create the Checkpoint configuration:

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

Run the following command to use `context.test_yaml_config(...)` to test the syntax:

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

A message appears indicating that the Checkpoint contains no validations. This is expected because they are passed in at runtime with `context.run_checkpoint(...)`.
