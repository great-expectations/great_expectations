The Google Cloud Storage examples use `google-cloud-storage`. `storage.Client()` finds credentials with Application Default Credentials. It checks the `GOOGLE_APPLICATION_CREDENTIALS` environment variable, then the file that `gcloud auth application-default login` creates, then the service account attached to the resource the code runs on. See [Application Default Credentials](https://docs.cloud.google.com/docs/authentication/application-default-credentials) in the Google Cloud documentation.

Replace `my-bucket` with the name of your bucket. `project_prefix` is where the project's files go in the bucket, and `data_docs_prefix` is where the Data Docs site goes. Then add the following code:

```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_gcs.py - define the storage location"
```

The following functions copy files between a local folder and the bucket, and the next steps use them. `upload_files()` keeps each file's path relative to the local folder. The client sets each file's content type from the file name. A browser needs the content type to display the Data Docs pages. `download_project()` skips `uncommitted/` unless you pass `include_results=True`. It also skips the empty placeholder objects that the Google Cloud console creates for folders. Objects whose names contain a `..` segment could replace a configuration file or land outside the download folder, so it skips those too.

```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_gcs.py - define transfer functions"
```
