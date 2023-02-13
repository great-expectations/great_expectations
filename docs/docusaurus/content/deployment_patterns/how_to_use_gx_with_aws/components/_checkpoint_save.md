import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CheckpointSaveTabPython from './_checkpoint_save_tab_python.md'
import CheckpointSaveTabYaml from './_checkpoint_save_tab_yaml.md'

After using `context.test_yaml_config(...)` to verify that all is well, we can add the Checkpoint to our Data Context:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

  <TabItem value="yaml">
  
<CheckpointSaveTabYaml />
  
  </TabItem>

  <TabItem value="python">
  
<CheckpointSaveTabPython />
  
  </TabItem>
</Tabs>