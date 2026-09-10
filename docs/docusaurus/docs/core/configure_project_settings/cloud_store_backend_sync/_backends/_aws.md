## Amazon S3

**Prerequisites:**
- An S3 bucket (e.g., `s3://my-gx-bucket/`)
- AWS CLI installed and configured (`aws configure`)
- The `boto3` Python package (optional, for programmatic access)

**Step 1: Pull config from S3**

```bash
aws s3 cp s3://my-gx-bucket/gx/ ./gx/ --recursive
```

**Step 2: Run your validation locally**

```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/cloud_store_backend_sync/_examples/cloud_store_backend_aws.py - run validation locally"
```

**Step 3: Push results back to S3**

Use a unique key (e.g., suite name + timestamp) to avoid collisions:

```bash
RUN_KEY="my_suite_$(date +%Y%m%d_%H%M%S)"
aws s3 cp gx/uncommitted/validations/my_suite/ s3://my-gx-bucket/gx/uncommitted/validations/my_suite/$RUN_KEY/ --recursive
```

**Step 4: Optional — Build and upload Data Docs**

```bash
python -c "import great_expectations as gx; context = gx.get_context(mode='file'); context.build_data_docs()"
aws s3 cp gx/uncommitted/data_docs/local_site/ s3://my-gx-bucket/gx/uncommitted/data_docs/local_site/ --recursive
```