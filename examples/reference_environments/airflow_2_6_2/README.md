# Airflow 2.6.2 Reference Environment

This airflow environment contains the airflow tutorial content along with a dag called `tutorial_dag_with_gx` that uses Great Expectations to create and run a simple data quality check on some data in motion. In the `dags` folder in this repo is the dag definition (`dag.py`) which uses a pandas dataframe as the source data with some simple data to create a Great Expectations `ExpectationSuite` and then run it using a `Checkpoint`.

This reference environment can be modified based on your needs. For example, you can add your own DAGs to the `dags` folder (e.g. a dag referencing data at rest e.g. in a database or blob store), or add your own plugins to the `plugins` folder. See also the `Dockerfile` for other options for modifications including changing the GX version, airflow version, python version or adding additional python packages. If you change the airflow version, you may need to also update the compose.yaml file with the version corresponding to your airflow version [see the official Airflow Docker Compose example documentation for more information](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

## Running the reference environment

To run the reference environment, run the following command from the repo root:

```bash
great_expectations example airflow
```

To access the Airflow UI, go to http://localhost:8080/ and use the default username/password: `airflow`/`airflow`.

In the UI, you can run the dag named `tutorial_dag_with_gx`


This environment was built using the [official Airflow Docker Compose example](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) with minor modifications.
