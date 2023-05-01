import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

Verify your new <TechnicalTag tag="datasource" text="Datasource" /> by loading data from it into a <TechnicalTag tag="validator" text="Validator" /> using a <TechnicalTag tag="batch_request" text="Batch Request" />.

We will use the `build_batch_request(...)` method of our Data Asset to generate a Batch Request. Here we have specified the `batch_request_options` property to only include the Batches where the `year` property is `2021`.

```python name="tests/integration/docusaurus/deployment_patterns/aws_cloud_storage_spark.py get_batch_request
```

Create an `ExpectationSuite` and then load data into the `Validator`.

```python name="tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/inferred_and_runtime_yaml_example.py get validator 1"
```

</TabItem>

<TabItem value="batch_request">

Add the name of the <TechnicalTag tag="data_asset" text="Data Asset" /> to the `data_asset_name` in your `BatchRequest`.

```python name="tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/inferred_and_runtime_yaml_example.py batch request 2"
```

Then load data into the `Validator`.

```python name="tests/integration/docusaurus/connecting_to_your_data/cloud/s3/spark/inferred_and_runtime_yaml_example.py get validator 2"
```

</TabItem>

</Tabs>