# S3 Reference Environment

This reference environment spins up one container which contains a jupyter notebook server and some example code.

The example code demonstrates how to use Great Expectations with data stored in S3 and by default uses the `nyc-tlc` bucket that is hosted by Amazon Registry of Open Data on AWS: https://registry.opendata.aws/nyc-tlc-trip-records-pds/  It also contains a notebook with a quickstart example that optionally uses an s3 bucket to store expectation suites, validation results and data docs (instead of storing them on the filesystem).


As of 2023-05-23 this note was posted: Note: access to this dataset is free, however direct S3 access does require an AWS account.

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
