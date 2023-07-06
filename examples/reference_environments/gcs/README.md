# GCS Reference Environment

This reference environment spins up two containers:

- A jupyter notebook server
- A server to host data docs (to view navigate to http://127.0.0.1:3000/)

The example code demonstrates how to use Great Expectations with data stored in Google Cloud Storage and by default uses data in the `taxi_reference_data`
that is hosted in the project defined by `GCP_PROJECT_NAME` env variable. It also contains a notebook with a quickstart example that optionally uses a GCS bucket to store expectation suites, validation results and data docs (instead of storing them on the filesystem). To use this notebook, set the name of your bucket in the `GCS_METADATA_STORES_BUCKET_NAME` environment variable on your host machine, or modify the notebook (see instructions in the notebook).

You can connect to GCS by setting up 2 environment variables:

First, the `GOOGLE_APPLICATION_CREDENTIALS` variable is used for authentication, and can point to a credential configuration JSON file.
The second variable needed is the `GCP_PROJECT_NAME`.

More information on authentication can be found in the [Google Cloud Authentication Documentation](https://cloud.google.com/docs/authentication/application-default-credentials#GAC).

Please note that unless you take steps to save data, your notebook changes could be lost when you stop the reference environment.

To copy data out of the container, you can use the `docker cp` command. For example to copy a notebook:

```bash
docker ps
```

`docker ps` will help you get the container id for use in the next command.

```bash
docker cp jupyter_container_id:/gx/my_notebook.ipynb .
```

Please also note that the jupyter notebook will use the default port, so please make sure you don't have anything else running on those ports, or take steps to avoid port conflicts.
