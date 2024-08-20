
A Google Cloud Service Data Doc site requires the following `store_backend` information:

- `class_name`: The name of the class to implement for accessing the target environment.  For Google Cloud Storage this will be `TupleGCSStoreBackend`.
- `project`: The name of the GCS project that will host the Data Docs site.
- `bucket`: The name of the bucket that will contain the Data Docs pages.
- `prefix`: The path of the folder that will contain the Data Docs pages relative to the root of the GCS bucket.  The combination of `bucket` and `prefix` must be unique accross all Stores used by a Data Context.

To add a local or networked filesystem backend for your Data Docs configuration, update the values of `project`, `bucket`, and `prefix` in the following code and execute it:

```python name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_gcs.py - add store backend"
```

For GX to access your Google Cloud Services environment, you will also need to configure the appropriate credentials.  By default, GCS credentials are handled through the gcloud command line tool and the `GOOGLE_APPLICATION_CREDENTIALS` environment variable. The gcloud command line tool is used to set up authentication credentials, and the `GOOGLE_APPLICATION_CREDENTIALS` environment variable provides the path to the json file with those credentials.

For more information on using the gcloud command line tool, see Google Cloud's [Cloud Storage client libraries](https://cloud.google.com/storage/docs/reference/libraries) documentation.