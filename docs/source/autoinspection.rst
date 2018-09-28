.. _autoinspection:

================================================================================
Autoinspection
================================================================================

It can be very convenient to have great expectations automatically review a \
dataset and suggest expectations that may be appropriate. Currently, there \
a very basic, but easily extensible, autoinspection capability available.

Dataset objects have an `autoinspect` method which allows you to provide a \
function that will evaluate a dataset object and add expectations to it. \
By default `autoinspect` will call the autoinspect function \
:func:`columns_exist <great_expectations.dataset.autoinspect.columns_exist>` \
which will add an `expect_column_to_exist` expectation for each column \
currently present on the dataset.

To implement additional autoinspection functions, you simply take a single \
parameter, a Dataset, and evaluate and add expectations to that object.


.. code-block:: python

    >> import great_expectations as ge
    >> df = ge.dataset.PandasDataset({"col": [1, 2, 3, 4, 5]})
    >> df.autoinspect(ge.dataset.autoinspect.columns_exist)
    >> df.get_expectations_config()
        {'dataset_name': None,
         'meta': {'great_expectations.__version__': '0.4.4__develop'},
         'expectations': [
             {'expectation_type': 'expect_column_to_exist',
              'kwargs': {'column': 'col'}
             }]
        }
