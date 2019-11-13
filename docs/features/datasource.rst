.. _datasource:

##############
Datasources
##############

Datasources are responsible for connecting data and compute infrastructure. Each Datasource provides
Great Expectations DataAssets (or batches in a DataContext) connected to a specific compute environment, such as a
SQL database, a Spark cluster, or a local in-memory Pandas DataFrame. Datasources know how to access data from
relevant sources such as an existing object from a DAG runner, a SQL database, an S3 bucket, GCS, or a local filesystem.

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

When adding custom expectations by subclassing an existing DataAsset type, use the data_asset_type parameter
to configure the datasource to load and return DataAssets of the custom type.

See :ref:`batch_generator` for more detail about how batch generators interact with datasources and DAG runners.

See datasource module docs :ref:`datasource_module` for more detail about available datasources.

