Run the following code to create the Checkpoint:

```python name="tests/integration/docusaurus/deployment_patterns/aws_cloud_storage_pandas.py create_checkpoint"
```

The Checkpoint is named `my_checkpoint`. It includes a Validation, using the `BatchRequest` you created earlier, and an `ExpectationSuite` containing two Expectations, `test_suite`.
