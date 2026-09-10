## Google Cloud Storage (GCS)

**Prerequisites:**
- A GCS bucket (e.g., `gs://my-gx-bucket/`)
- `gsutil` CLI installed and authenticated (`gcloud auth login`)
- The `google-cloud-storage` Python package (optional, for programmatic access)

**Step 1: Pull config from GCS**

```bash
gsutil -m cp -r gs://my-gx-bucket/gx/ ./gx/
```

**Step 2: Run your validation locally**

```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/cloud_store_backend_sync/_examples/cloud_store_backend_gcs.py - run validation locally"
```

**Step 3: Push results back to GCS**

Use a unique key (e.g., suite name + timestamp) to avoid collisions:

```bash
RUN_KEY="my_suite_$(date +%Y%m%d_%H%M%S)"
gsutil -m cp -r gx/uncommitted/validations/my_suite/ gs://my-gx-bucket/gx/uncommitted/validations/my_suite/$RUN_KEY/
```

**Step 4: Optional — Build and upload Data Docs**

```bash
python -c "import great_expectations as gx; context = gx.get_context(mode='file'); context.build_data_docs()"
gsutil -m cp -r gx/uncommitted/data_docs/local_site/ gs://my-gx-bucket/gx/uncommitted/data_docs/local_site/
```