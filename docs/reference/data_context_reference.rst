.. _data_context_reference:

############################
Data Context Reference
############################

A :ref:`data_context` manages assets for a project.

*************************
Configuration
*************************


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

Stores provide a valuable abstraction for making access to critical resources such as expectation suites, validation
results, profiling data, data documentation, and evaluation parameters both easy to configure and to extend and
customize. See the :ref:`stores_reference` for more information.



Secrets
========


Config Variables File
-----------------------


Environment Variable Substitution
----------------------------------

