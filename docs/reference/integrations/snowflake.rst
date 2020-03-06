.. _snowflake:

##############
Snowflake
##############

When using the snowflake dialect, `SqlAlchemyDataset` will create a **transient** table instead of a **temporary**
table when passing in `query` Batch Kwargs or provding `custom_sql` to its constructor. Consequently, users **must**
provide a `table_name` in addition to the `query` parameter.
