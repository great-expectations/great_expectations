To install Great Expectations with the optional dependencies needed to work with AWS S3 we execute the following pip command from the terminal:

```bash title="Terminal input"
python -m pip install 'great_expectations[s3]'
```

This will install Great Expectations and the `boto3` package.  GX uses `boto3` to access S3.