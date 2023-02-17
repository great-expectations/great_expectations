import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Using this example configuration, add in your S3 bucket and path to a directory that contains some of your data:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

<TabItem value="yaml">

```python name="tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/inferred_and_runtime_yaml_example.py datasource config"
```

Run this code to test your configuration.

```python name="tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/inferred_and_runtime_yaml_example.py test datasource config"
```

</TabItem>

<TabItem value="python">

```python name="tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/inferred_and_runtime_python_example.py datasource config"
```

Run this code to test your configuration.

```python name="tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/inferred_and_runtime_python_example.py test datasource config"
```

</TabItem>

</Tabs>

If you specified an S3 path containing CSV files you will see them listed as `Available data_asset_names` in the output of `test_yaml_config()`.

Feel free to adjust your configuration and re-run `test_yaml_config()` as needed.