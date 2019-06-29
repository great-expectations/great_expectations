.. _profiling:

================================================================================
Profiling
================================================================================

It can be very convenient to have great expectations automatically review a \
dataset and suggest expectations that may be appropriate. Currently, there's \
a very basic, but easily extensible, profiling capability available.

Dataset objects have a `profile` method which allows you to provide a \
profiler class that will evaluate a dataset object and add expectations to it.

.. code-block:: python

    >> import great_expectations as ge
    >> df = ge.dataset.PandasDataset({"col": [1, 2, 3, 4, 5]})
    >> df.profile(ge.profile.ColumnsExistProfiler)
    >> df.get_expectation_suite()
        {'data_asset_name': None,
         'expectation_suite_name': None,
         'meta': {'great_expectations.__version__': '0.7.0'},
         'expectations': [
             {'expectation_type': 'expect_column_to_exist',
              'kwargs': {'column': 'col'}
             }]
        }
