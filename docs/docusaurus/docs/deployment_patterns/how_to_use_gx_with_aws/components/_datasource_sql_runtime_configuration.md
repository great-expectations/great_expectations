import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


Using the following example configuration, add in the `CONNECTION_STRING` for your database:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

  <TabItem value="yaml">

```python name="tests/integration/db/awsathena.py Datasource YAML config"

```

  </TabItem>

  <TabItem value="python">

```python name="tests/integration/docusaurus/connecting_to_your_data/database/athena_python_example.py Datasource dict config"
```
  
  </TabItem>
</Tabs>
