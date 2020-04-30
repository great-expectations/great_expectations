.. _snowflake:

##############
Snowflake
##############

When using the snowflake dialect, `SqlAlchemyDataset` will create a **transient** table instead of a **temporary**
table when passing in `query` Batch Kwargs or providing `custom_sql` to its constructor. Consequently, users **must**
provide a `snowflake_transient_table` in addition to the `query` parameter. Any existing table with that name will be
overwritten.
