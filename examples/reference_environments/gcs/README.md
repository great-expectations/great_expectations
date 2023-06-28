# GCS Reference Environment

This reference environment spins up one container which contains a jupyter notebook server and some example code.

The example code demonstrates how to use Great Expectations with data stored in Google Cloud Storage and by default uses data in the `taxi_reference_data` 
that is hosted in the project defined by `GCP_PROJECT_NAME` env variable.

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
