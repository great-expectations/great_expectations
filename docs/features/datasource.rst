.. _datasource:

##############
Datasources
##############

Datasources are responsible for connecting data and compute infrastructure. Each Datasource provides
Great Expectations Data Assets connected to a specific compute environment, such as a
SQL database, a Spark cluster, or a local in-memory Pandas DataFrame. Datasources know how to access data from
relevant sources such as an existing object from a DAG runner, a SQL database, an S3 bucket, GCS, or a local filesystem.

To bridge the gap between those worlds, Datasources can interact closely with :ref:`batch_kwargs_generator` which
are aware of a source of data and can produce produce identifying information, called
"batch_kwargs" that datasources can use to get individual batches of data.

See :ref:`datasource_reference` for more detail about configuring and using datasources in your DataContext.

See datasource module docs :ref:`datasource_module` for more detail about available datasources.


