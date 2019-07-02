.. _datasources:

Datasources
============

Datasources are responsible for connecting to data infrastructure. Each Datasource is a source
of materialized data, such as a SQL database, S3 bucket, or local file directory.

Each Datasource also provides access to Great Expectations data assets that are connected to
a specific compute environment, such as a SQL database, a Spark cluster, or a local in-memory
Pandas Dataframe.

To bridge the gap between those worlds, Datasources interact closely with *generators* which
are aware of a source of data and can produce produce identifying information, called
"batch_kwargs" that datasources can use to get individual batches of data. They add flexibility
in how to obtain data such as with time-based partitioning, downsampling, or other techniques
appropriate for the datasource.

For example, a generator could produce a SQL query that logically represents "rows in the Events
table with a timestamp on February 7, 2012," which a SqlAlchemyDatasource could use to materialize
a SqlAlchemyDataset corresponding to that batch of data and ready for validation.

Since opinionated DAG managers such as airflow, dbt, prefect.io, dagster can also act as datasources
and/or generators for a more generic datasource.

See datasource module docs :ref:`datasource_module`
