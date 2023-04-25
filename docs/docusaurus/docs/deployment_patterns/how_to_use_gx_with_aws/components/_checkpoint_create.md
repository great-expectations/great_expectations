We create the Checkpoint configuration:

```python name="tests/integration/docusaurus/deployment_patterns/aws_cloud_storage_pandas.py create_checkpoint"
```

We have named the checkpoint `my_checkpoint`, and added one Validation, using the `BatchRequest` we created earlier, 
and referring to the `ExpectatinSuite` with our 2 Expectations, `test_suite`.
