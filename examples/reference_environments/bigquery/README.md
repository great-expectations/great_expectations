# BigQuery Reference Environment

This reference environment spins up two containers:

- A jupyter notebook server
- A server to host data docs (to view navigate to http://127.0.0.1:3000/)

The jupyter notebook server contains a notebook with a quickstart for using Great Expectations with BigQuery.

GX will use the `sqlalchemy-bigquery` connector to connect to BigQuery.

You can connect to BigQuery by setting up 2 environment variables:

First, the `GOOGLE_APPLICATION_CREDENTIALS` variable is used for authentication, and can point to a credential configuration JSON file.
The second variable needed is the `BIGQUERY_CONNECTION_STRING`, which has the format `bigquery://gcp_project/bigquery_dataset`.

More information on authentication can be found in the [Google Cloud Authentication Documentation](https://cloud.google.com/docs/authentication/application-default-credentials#GAC).

Please also note that the jupyter notebook will use the default ports, so please make sure you don't have anything else running on those ports, or take steps to avoid port conflicts.

The container serving the jupyter notebook will install Great Expectations with the appropriate dependencies based on the latest release, then install from the latest on the `develop` branch. To modify this e.g. install from the latest release only or from a different branch, edit `jupyter.Dockerfile`.
