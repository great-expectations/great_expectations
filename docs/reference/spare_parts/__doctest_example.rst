################
Doctest Examples
################

Use these examples during migration to testable docs.

The block below is not rendered in the final documentation, but *does* affect the namespace.

.. invisible-code-block: python

    import great_expectations as ge
    import pandas as pd
    npi = ge.dataset.PandasDataset({"provider_id": [1,2,3]})
    from great_expectations.core import ExpectationValidationResult, ExpectationConfiguration
    res = npi.expect_column_values_to_be_unique("provider_id")


This block is a standard doctest block, with one statement.

>>> npi.expect_column_values_to_be_unique("provider_id") == ExpectationValidationResult(
...  **{
...    "result": {
...      "element_count": 3,
...      "missing_count": 0,
...      "missing_percent": 0.0,
...      "unexpected_count": 0,
...      "unexpected_percent": 0.0,
...      "unexpected_percent_nonmissing": 0.0,
...      "partial_unexpected_list": []
...    },
...    "success": True,
...    "exception_info": None,
...    "meta": {},
...    "expectation_config": ExpectationConfiguration(**{
...      "expectation_type": "expect_column_values_to_be_unique",
...      "meta": {},
...      "kwargs": {
...        "column": "provider_id",
...        "result_format": "BASIC"
...      }
...    })
...  })
True


This block is tested only in that it must not raise an exception. No output test happens from a code-block.

.. code-block:: python

    assert npi.expect_column_values_to_be_unique("provider_id") != ExpectationValidationResult(
        meta={},
        result={
            "element_count": 3,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "unexpected_percent_nonmissing": 0.0,
            "partial_unexpected_list": []
        },
        success=True,
        expectation_config=ExpectationConfiguration(**{
            "expectation_type": "expect_column_values_to_be_unique",
            "meta": {},
            "kwargs": {
                "column": "provider_id",
                "result_format": "BASIC"
            }
        }),
        exception_info=None
    )


These three lines will be evaluated as classic doctest:

>>> df = pd.read_csv("/opt/data/titanic/Titanic.csv")
>>> df = ge.dataset.PandasDataset(df)
>>> res = df.expect_column_values_to_be_in_set("Sex", ["male", "female"])

This section would often fail, but will be skipped because of the Sphinx comment. It **will** be rendered.

.. skip: next

>>> print(res)
    {
      "exception_info": null,
      "success": true,
      "meta": {},
      "result": {
        "element_count": 1313,
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "unexpected_percent_nonmissing": 0.0,
        "partial_unexpected_list": []
      },
      "expectation_config": {
        "expectation_type": "expect_column_values_to_be_in_set",
        "meta": {},
        "kwargs": {
          "column": "Sex",
          "value_set": [
            "male",
            "female"
          ],
          "result_format": "BASIC"
        }
      }
    }

