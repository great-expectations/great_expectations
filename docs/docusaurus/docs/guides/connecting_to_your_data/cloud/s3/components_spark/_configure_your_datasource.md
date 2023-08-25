import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

Using this example configuration, add in your S3 bucket and path to a directory that contains some of your data:

```python name="tests/integration/docusaurus/deployment_patterns/aws_cloud_storage_spark.py add_s3_datasource"
```

In the example, we have added a Data Source that connects to data in S3 using a Spark dataframe. The name of
the new datasource is ``s3_datasource`` and it refers to a S3 bucket named ``taxi-data-sample-test``.

