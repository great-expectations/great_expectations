.. _data_context:

DataContexts
===================

A DataContext represents a Great Expectations project. It organizes storage and access for
expectation suites, datasources, notification settings, and data fixtures.

The DataContext is configured via a yml file stored in a directory called great_expectations; the configuration file
as well as managed expectation suites should be stored in version control.

DataContexts use data sources you're already familiar with. Generators help introspect data stores and data execution
frameworks (such as airflow, Nifi, dbt, or dagster) to describe and produce batches of data ready for analysis. This
enables fetching, validation, profiling, and documentation of your data in a way that is meaningful within your
existing infrastructure and work environment.

DataContexts use a datasource-based namespace, where each accessible type of data has a three-part
normalized *data_asset_name*, consisting of *datasource/generator/generator_asset*.

- The datasource actually connects to a source of data and returns Great Expectations DataAssets \
  connected to a compute environment and ready for validation.

- The Generator knows how to introspect datasources and produce identifying "batch_kwargs" that define \
  particular slices of data.

- The generator_asset is a specific name -- often a table name or other name familiar to users -- that \
  generators can slice into batches.

An expectation suite is a collection of expectations ready to be applied to a batch of data. Since
in many projects it is useful to have different expectations evaluate in different contexts--profiling
vs. testing; warning vs. error; high vs. low compute; ML model or dashboard--suites provide a namespace
option for selecting which expectations a DataContext returns.

In many simple projects, the datasource or generator name may be omitted and the DataContext will infer
the correct name when there is no ambiguity.

Similarly, if no expectation suite name is provided, the DataContext will assume the name "default".

Configuration
---------------

The DataContext configuration file provides fine-grained control over several core features available to the
DataContext to assist in managing resources used by Great Expectations. Key configuration areas include specifying
datasources, data documentation, and "stores" used to manage access to resources such as expectation suites,
validation results, profiling results, evaluation parameters and plugins.

Datasources
_____________

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

Note that the datasources section *includes* all defined generators as well as specifying their names.

Data Documentation
____________________

The :ref:`data_documentation` section defines how individual sites should be built and deployed. See the detailed
documentation for more information.

Stores
_______

Stores are undergoing a significant redesign. Please reach out on slack for more information on configuring stores!
