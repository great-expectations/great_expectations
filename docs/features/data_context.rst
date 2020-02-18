.. _data_context:


################
DataContexts
################

A DataContext represents a Great Expectations project. It organizes storage and access for
expectation suites, datasources, notification settings, and data fixtures.

The DataContext is configured via a yml file stored in a directory called great_expectations; the configuration file
as well as managed expectation suites should be stored in version control.

DataContexts manage connections to your data and compute resources, and support integration with execution
frameworks (such as airflow, Nifi, dbt, or dagster) to describe and produce batches of data ready for analysis. Those
features enable fetching, validation, profiling, and documentation of your data in a way that is meaningful within your
existing infrastructure and work environment.

DataContexts also manage Expectation Suites. Expectation Suites combine multiple Expectation Configurations into an
overall description of a dataset. Expectation Suites should have names corresponding to the kind of data they
define, like “NPI” for National Provider Identifier data or “company.users” for a users table.

The DataContext also provides other services, such as storing and substituting evaluation parameters during validation.
See :ref:`data_context_evaluation_parameter_store` for more information.

See the :ref:`data_context_reference` for more information.