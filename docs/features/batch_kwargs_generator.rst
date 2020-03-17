.. _batch_kwargs_generator:

########################
Batch Kwargs Generators
########################

Batch Kwargs are specific instructions for a Datasource about what data should be prepared as a “batch” for
validation. The batch could be a specific database table, the most recent log file delivered to S3, or even a subset
of one of those objects such as the first 10,000 rows.

A BatchKwargsGenerator builds those instructions for GE datasources by inspecting storage backends or data, or by
maintaining configuration such as commonly-used paths or filepath conventions. That allows BatchKwargsGenerators to add
flexibility in how to obtain data such as by exposing time-based partitions or sampling data.

For example, a Batch Kwargs Generator could be **configured** to produce a SQL query that logically represents "rows in
the Events table with a type code of 'X' that occurred within seven days of a given timestamp."  With that
configuration, you could provide a timestamp as a partition name, and the Batch Kwargs Generator will produce
instructions that a SQLAlchemyDatasource could use to materialize a SQLAlchemyDataset corresponding to that batch of
data and ready for validation.

********
Batch
********

A batch is a sample from a data asset, sliced according to a particular rule.
For example, an hourly slide of the Events table or “most recent `users` records.”

A Batch is the primary unit of validation in the Great Expectations DataContext.
Batches include metadata that identifies how they were constructed--the same “batch_kwargs”
assembled by the generator, "batch_markers" that provide more detailed metadata to aid in replicating complicated
workflows, and optionally "batch_parameters" that include information such as an asset or partition name.

See more detailed documentation on the :ref:`generator_module`.
