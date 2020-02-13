.. _batch_generator:

##################
Batch Generators
##################

A generator builds instructions for GE datasources by inspecting data and helping to identify useful elements such as
batches. Batch generators produce identifying information, called "batch_kwargs" that datasources can use to get
individual batches of data. They add flexibility in how to obtain data such as with time-based partitioning,
downsampling, or other techniques appropriate for the datasource.

For example, a generator could produce a SQL query that logically represents "rows in
the Events table with a timestamp on February 7, 2012," which a SQLAlchemyDatasource
could use to materialize a SQLAlchemyDataset corresponding to that batch of data and
ready for validation.

********
Batch
********

A batch is a sample from a data asset, sliced according to a particular rule.
For example, an hourly slide of the Events table or “most recent `users` records.”

A Batch is the primary unit of validation in the Great Expectations DataContext.
Batches include metadata that identifies how they were constructed--the same “batch_kwargs”
assembled by the generator, While not every datasource will enable re-fetching a
specific batch of data, GE can store snapshots of batches or store metadata from an
external data version control system.

See more detailed documentation on the :ref:`generator_module`.

*last updated*: |lastupdate|
