.. _data_asset_features:


############################
Data Asset Features Guide
############################

This document describes useful features of the DataAsset object. A :ref:`DataAsset <data_asset_module>` in Great
Expectations is the root class that enables declaring and validating expectations; it brings together data and
expectation evaluation logic.

***********************
Interactive Evaluation
***********************

Setting the `interactive_evaluation` flag on a DataAsset make it possible to declare expectations and store
expectations without immediately evaluating them. When interactive evaluation is disabled, the running an
expectation method on a DataAsset will return the configuration just added to its expectation suite rather than a
result object.

At initialization
==================

.. code-block:: python

    >> import great_expectations as ge
    >> import pandas as pd
    >> df = pd.read_csv("./tests/examples/titanic.csv")
    >> ge_df = ge.dataset.PandasDataset(df, interactive_evaluation=False)
    >> ge_df.expect_column_values_to_be_in_set('Sex', ["male", "female"])

    {
        'stored_configuration': {
            'expectation_type': 'expect_column_values_to_be_in_set',
            'kwargs': {
                'column': 'Sex',
                'value_set': ['male', 'female'],
                'result_format': 'BASIC'
            }
        }
    }

Dynamically adjusting interactive evaluation
=============================================

.. code-block:: python

    >> import great_expectations as ge
    >> import pandas as pd
    >> df = pd.read_csv("./tests/examples/titanic.csv")
    >> ge_df = ge.dataset.PandasDataset(df, interactive_evaluation=True)
    >> ge_df.expect_column_values_to_be_in_set('Sex', ["male", "female"])

    {
        'success': True,
        'result': {
            'element_count': 1313,
            'missing_count': 0,
            'missing_percent': 0.0,
            'unexpected_count': 0,
            'unexpected_percent': 0.0,
            'unexpected_percent_nonmissing': 0.0,
            'partial_unexpected_list': []
        }
    }

    >> ge_df.set_config_value("interactive_evaluation", False)
    >> ge_df.expect_column_values_to_be_in_set("PClass", ["1st", "2nd", "3rd"])

    {
      'stored_configuration': {
        'expectation_type': 'expect_column_values_to_be_in_set',
        'kwargs': {
          'column': 'PClass',
          'value_set': [
            '1st',
            '2nd',
            '3rd'
          ],
          'result_format': 'BASIC'
        }
      }
    }

*last updated*: |lastupdate|
