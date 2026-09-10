## Azure Blob Storage

**Prerequisites:**
- An Azure Blob container (e.g., `https://myaccount.blob.core.windows.net/my-gx-container/`)
- Azure CLI installed and authenticated (`az login`)
- The `azure-storage-blob` Python package (optional, for programmatic access)

**Step 1: Pull config from Azure Blob**

```bash
az storage blob download-batch \
  --account-name myaccount \
  --source my-gx-container \
  --pattern gx/* \
  --destination ./
```

**Step 2: Run your validation locally**

```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/cloud_store_backend_sync/_examples/cloud_store_backend_azure.py - run validation locally"
```

**Step 3: Push results back to Azure Blob**

Use a unique key (e.g., suite name + timestamp) to avoid collisions:

```bash
RUN_KEY="my_suite_$(date +%Y%m%d_%H%M%S)"
az storage blob upload-batch \
  --account-name myaccount \
  --destination my-gx-container \
  --source gx/uncommitted/validations/my_suite/ \
  --destination-path gx/uncommitted/validations/my_suite/$RUN_KEY/
```

**Step 4: Optional — Build and upload Data Docs**

```bash
python -c "import great_expectations as gx; context = gx.get_context(mode='file'); context.build_data_docs()"
az storage blob upload-batch \
  --account-name myaccount \
  --destination my-gx-container \
  --source gx/uncommitted/data_docs/local_site/ \
  --destination-path gx/uncommitted/data_docs/local_site/
```