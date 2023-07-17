# Azure Blob Store Reference Environment

This reference environment spins up two containers:

- A jupyter notebook server
- A server to host data docs (to view navigate to http://127.0.0.1:3000/)

The example code demonstrates how to use Great Expectations with data stored in Azure Blob Storage (ABS) and by default uses data in the ``
that is hosted in the project defined by `AZURE_STORAGE_ACOUNT_URL` env variable.

 It also contains a notebook with a quickstart example that optionally uses an ABS bucket to store expectation suites, validation results and data docs (instead of storing them on the filesystem). To use this notebook, set the name of your bucket in the `ABS_METADATA_STORES_CONTAINER_NAME` and `AZURE_CONNECTION_STRING` environment variables on your host machine, or modify the notebook (see instructions in the notebook).

You can connect to Azure Blob Store by setting up 2 environment variables:

First, the `AZURE_CONNECTION_STRING` and `AZURE_CREDENTIAL` variables are used for authentication, and `AZURE_STORAGE_ACCOUNT_URL`, which points to the storage account location.

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
