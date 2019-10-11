.. _data_context_reference:

############################
Data Context Reference
############################

A :ref:`data_context` manages assets for a project.

*************************
Configuration
*************************


The DataContext configuration file provides fine-grained control over several core features available to the
DataContext to assist in managing resources used by Great Expectations. Key configuration areas include specifying
datasources, data documentation, and "stores" used to manage access to resources such as expectation suites,
validation results, profiling results, evaluation parameters and plugins.

Datasources
=============


The `datasources` section declares which :ref:`datasource` objects should be available in the DataContext.
Each datasource definition should include the `class_name` of the datasource, generators, and any other relevant
configuration information. For example, the following simple configuration supports a Pandas-based pipeline:

.. code-block:: yaml

  datasources:
    pipeline:
      type: pandas
      generators:
        default:
          type: memory

The following configuration demonstrates a more complicated configuration for reading assets from s3 into pandas. It
will access the amazon public NYC taxi data and provides access to two assets: 'taxi-green' and 'taxi-fhv' which
represent two public datasets available from the resource.

.. code-block:: yaml

  datasources:
    nyc_taxi:
      type: pandas
      generators:
        s3:
          type: s3
          bucket: nyc-tlc
          delimiter: '/'
          reader_options:
            sep: ','
            engine: python
          assets:
            taxi-green:
              prefix: trip data/
              regex_filter: 'trip data/green.*\.csv'
            taxi-fhv:
              prefix: trip data/
              regex_filter: 'trip data/fhv.*\.csv'
      data_asset_type:
        class_name: PandasDataset

Note that the datasources section *includes* all defined generators as well as specifying their names. See
:ref:`custom_expectations_in_datasource` for more information about configuring datasources to use custom expectations.


Data Asset Names
===================

Data asset names consist of three parts, a datasource, generator, and generator asset. DataContext functions will
attempt to "normalize" a data_asset_name if they are provided with only a string, by splitting on the delimiter
character (by default '/') and then attempting to identify an unambiguous name. DataContext searches through
names that already have expectation suites first, then considers names provided by generators.

For example:

.. code-block:: python

    # Returns a normalized name with string representation my_datasource/my_generator/my_asset if
    # my_datasource and my_generator uniquely provide an asset called my_asset
    context.normalize_data_asset_name("my_asset")


Data Documentation
=====================

The :ref:`data_docs` section defines how individual sites should be built and deployed. See the detailed
documentation for more information.


Stores
=============

Stores provide a valuable abstraction for making access to critical resources such as expectation suites, validation
results, profiling data, data documentation, and evaluation parameters both easy to configure and to extend and
customize. See the :ref:`stores_reference` for more information.


.. _environment_and_secrets:

*****************************************
Managing Environment and Secrets
*****************************************

In a DataContext configuration, values that should come from the runtime environment or secrets can be injected via
a separate config file or using environment variables. Use the `${var}` syntax in a config file to specify a variable
to be substituted.

Config Variables File
========================

DataContext accepts a parameter called `config_variables_file_path` which can include a file path from which variables
to substitute should be read. The file needs to define top-level keys which are available to substitute into a
DataContext configuration file. Keys from the config variables file can be defined to represent complex types such as
a dictionary or list, which is often useful for configuring database access.

.. code-block:: yaml

  prod_credentials:
    type: postgresql
    host: secure_server
    port: 5432
    username: username
    password: sensitive_password
    database: ge

  dev_credentials:
    type: postgresql
    host: localhost
    port: 5432
    username: dev
    password: dev
    database: ge


Environment Variable Substitution
====================================

Environment variables will be substituted into a DataContext config with higher priority than values from the
config variables file.



