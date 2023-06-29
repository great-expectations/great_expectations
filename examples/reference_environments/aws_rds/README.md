# AWS RDS Reference Environment

This reference environment spins up one container which contains a jupyter notebook server and some example code.

The example code demonstrates how to use Great Expectations with data stored in an RDS Postgres database. 

You can connect to RDS by setting up 4 environment variables:

- `AWS_ACCESS_KEY_ID`: For authentication.
- `AWS_SECRET_ACCESS_KEY`: Also for authentication.
- `AWS_SESSION_TOKEN`: If using temporary authentication.
- `AWS_RDS_CONNECTION_STRING`: To connect to RDS database. For more information on how to format the connection string, please refer to the (AWS documentation.)[https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_CommonTasks.Connect.html]

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
