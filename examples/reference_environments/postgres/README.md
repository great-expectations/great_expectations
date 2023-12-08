# Postgres Reference Environment

This reference environment spins up three containers:

- A postgres database pre-loaded with a sample of one month of NYC taxi trip data
- A jupyter notebook server
- A server to host data docs (to view navigate to http://127.0.0.1:3000/)

The jupyter notebook server contains a notebook with a quickstart for using Great Expectations with postgres. It also contains a notebook with a quickstart example that optionally uses the database to store expectation suites and validation results (instead of storing them on the filesystem).

You can connect to the database using the following credentials:

- Outside the container on your host machine: "postgresql://example_user@localhost/gx_example_db"
- Inside the notebook: "postgresql://example_user@db/gx_example_db"

Please note that unless you take steps to save data, your notebook and database changes could be lost when you stop the reference environment.

To copy data out of the container, you can use the `docker cp` command. For example to copy a notebook:

```bash
docker ps
```

`docker ps` will help you get the container id for use in the next command.

```bash
docker cp jupyter_container_id:/gx/my_notebook.ipynb .
```

Please also note that the database and jupyter notebook will use the default ports, so please make sure you don't have anything else running on those ports, or take steps to avoid port conflicts.

The container serving the jupyter notebook will install Great Expectations with the appropriate dependencies based on the latest release, then install from the latest on the `develop` branch. To modify this e.g. install from the latest release only or from a different branch, edit `jupyter.Dockerfile`.
