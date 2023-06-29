# BigQuery Reference Environment

This reference environment spins up a single container:

- A jupyter notebook server

The jupyter notebook server contains a notebook with a quickstart for using Great Expectations with BigQuery.

GX will use the `sqlalchemy-bigquery` connector to connect to BigQuery. 

You can connect to BigQuery by setting up 2 environment variables:

First, the `GOOGLE_APPLICATION_CREDENTIALS` variable is used for authentication, and can point to a credential configuration JSON file. 
The second variable needed is the `BIGQUERY_CONNECTION_STRING`, which has the format `bigquery://gcp_project/bigquery_dataset`.

More information on authentication can be found in the [Google Cloud Authentication Documentation](https://cloud.google.com/docs/authentication/application-default-credentials#GAC).

Please also note that the jupyter notebook will use the default ports, so please make sure you don't have anything else running on those ports, or take steps to avoid port conflicts.
