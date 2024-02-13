import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

<TabItem value="yaml">

Put your connection string in this template:

```yaml title="YAML" name="version-0.18 docs/docusaurus/docs/snippets/redshift_yaml_example.py datasource config"
```

Run this code to test your configuration.

```python title="Python" name="version-0.18 docs/docusaurus/docs/snippets/redshift_yaml_example.py test datasource config"
```
</TabItem>

<TabItem value="python">

Put your connection string in this template:

```python title="Python" name="version-0.18 docs/docusaurus/docs/snippets/redshift_python_example.py datasource config"
```

Run this code to test your configuration.

```python title="Python" name="version-0.18 docs/docusaurus/docs/snippets/redshift_python_example.py test datasource config"
```

</TabItem>

</Tabs>

You will see your database tables listed as `Available data_asset_names` in the output of `test_yaml_config()`.

Feel free to adjust your configuration and re-run `test_yaml_config` as needed.